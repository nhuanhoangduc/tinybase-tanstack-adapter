import type { CollectionConfig, MutationFn, PendingMutation, UtilsRecord } from '@tanstack/db'
import type { Row, Store } from 'tinybase'

export type Item = { id: string }
export interface RowCodec<T extends Item> {
  /** Pure, synchronous. Must preserve id and round-trip all stored cells. */
  decode(id: string, row: Row): T
  /** Pure, synchronous. Return cells only; id lives in the TinyBase row key. */
  encode(item: T): Row
}
export interface TinyBaseOptions<T extends Item> {
  store: Store
  tableId: string
  id?: string
  codec?: RowCodec<T>
  /** Optional synchronous domain validation, used for local AND remote rows. */
  validate?: (item: T) => void
  /** Load your persister before initial sync. Rejections fail collection.preload(). */
  ready?: Promise<unknown>
  /** Remote decoding/sync errors. Keep this callback non-throwing. */
  onError?: (error: Error) => void
}
export interface TinyBaseUtils extends UtilsRecord {
  getSyncError(): Error | undefined
  resync(): void
}

type Plan = { table: string; key: string; before?: Row; after?: Row }
type Binding = {
  store: Store
  prepare(mutation: PendingMutation<object>): Plan
  assertHealthy(): void
}
const bindings = new WeakMap<object, Binding>()
const own = (obj: object, key: string) => Object.hasOwn(obj, key)
const asError = (value: unknown) => value instanceof Error ? value : new Error(String(value))
const equalRows = (a: Row | undefined, b: Row | undefined) =>
  a === undefined || b === undefined ? a === b :
    Object.keys(a).length === Object.keys(b).length &&
    Object.keys(a).every(key => own(b, key) && Object.is(a[key], b[key]))

function checkRow(row: Row): Row {
  if (!row || typeof row !== 'object' || Array.isArray(row)) throw new Error('Codec must return a Row')
  const entries = Object.entries(row)
  if (!entries.length) throw new Error('TinyBase cannot store an empty row; provide at least one cell')
  for (const [key, value] of entries) {
    if (!key || key === 'id') throw new Error('Cell names must be nonempty; "id" is reserved for the row key')
    if (!['string', 'number', 'boolean'].includes(typeof value) ||
        (typeof value === 'number' && !Number.isFinite(value))) {
      throw new Error(`Invalid cell "${key}": use a codec for null, objects, arrays, dates or undefined`)
    }
  }
  return Object.fromEntries(entries)
}

/** Shared mutationFn for explicit TanStack transactions, including multiple tables.
 * All participating collections must use this adapter and the SAME TinyBase Store.
 * Resolves when accepted in memory, NOT when persisted to disk or synced remotely.
 */
export const tinybaseMutationFn: MutationFn = async ({ transaction }) => {
  let store: Store | undefined
  const seen = new Map<string, Set<string>>()
  const plans = transaction.mutations.map(mutation => {
    const binding = bindings.get(mutation.collection)
    if (!binding) throw new Error('Transaction contains an inactive or non-TinyBase collection; preload first')
    binding.assertHealthy()
    if (store && store !== binding.store) throw new Error('Atomic transactions require the same TinyBase Store')
    store = binding.store
    const plan = binding.prepare(mutation as PendingMutation<object>)
    const keys = seen.get(plan.table) ?? new Set<string>()
    if (keys.has(plan.key)) throw new Error('Multiple collections mutate the same TinyBase row in one transaction')
    keys.add(plan.key)
    seen.set(plan.table, keys)
    return plan
  })
  if (!store) return
  const target = store
  let failure: Error | undefined
  // Validate the whole batch before writing. Roll back silently rejected schema writes.
  target.transaction(() => {
    try {
      for (const plan of plans) {
        if (!plan.after) target.delRow(plan.table, plan.key)
        else if (!plan.before) target.setRow(plan.table, plan.key, plan.after)
        else {
          for (const cell of Object.keys(plan.before)) {
            if (!own(plan.after, cell)) target.delCell(plan.table, plan.key, cell)
          }
          for (const [cell, value] of Object.entries(plan.after)) {
            if (!Object.is(plan.before[cell], value)) target.setCell(plan.table, plan.key, cell, value)
          }
        }
      }
    } catch (error) { failure = asError(error) }
  }, () => {
    for (const plan of plans) {
      const actual = target.hasRow(plan.table, plan.key) ? target.getRow(plan.table, plan.key) : undefined
      if (!equalRows(actual, plan.after)) failure ??= new Error(`TinyBase rejected or transformed ${plan.table}/${plan.key}`)
    }
    return failure !== undefined
  })
  if (failure) throw failure
  // Do not await sync commit receipts here: TanStack queues them behind this mutation.
}

/** One collection per TinyBase table. Eager in-memory sync; no SQLite query pushdown. */
export function tinybaseCollectionOptions<T extends Item>(
  options: TinyBaseOptions<T>,
): CollectionConfig<T, string, never, TinyBaseUtils> & { utils: TinyBaseUtils } {
  const { store, tableId, validate } = options
  if (!tableId) throw new Error('tableId must not be empty')
  const codec: RowCodec<T> = options.codec ?? {
    decode: (id, row) => ({ ...row, id }) as T,
    encode: item => Object.fromEntries(Object.entries(item).filter(([key, value]) =>
      key !== 'id' && !['$key', '$collectionId', '$origin', '$synced'].includes(key) && value !== undefined)) as Row,
  }
  const encode = (item: T) => {
    if (typeof item.id !== 'string' || !item.id) throw new Error('id must be a nonempty string')
    validate?.(item)
    return checkRow(codec.encode(item))
  }
  const decode = (key: string, row: Row) => {
    if (own(row, 'id')) throw new Error('TinyBase cells must not contain the reserved id field')
    const item = codec.decode(key, { ...row })
    if (item.id !== key) throw new Error('Codec changed the row id')
    validate?.(item)
    if (!equalRows(encode(item), row)) throw new Error(`Codec does not round-trip ${tableId}/${key}`)
    return item
  }
  let claimed = false
  let storeBusy = false
  let syncError: Error | undefined
  let resync: (() => void) | undefined
  const binding: Binding = {
    store,
    assertHealthy() {
      if (!resync) throw new Error('Collection is not ready; await preload() first')
      if (syncError) throw syncError
      if (storeBusy) throw new Error('Do not mutate a collection inside a TinyBase transaction or listener')
    },
    prepare(mutation) {
      const key = mutation.key as string
      if (typeof key !== 'string' || !key) throw new Error('Invalid mutation key')
      const before = store.hasRow(tableId, key) ? store.getRow(tableId, key) : undefined
      if (mutation.type === 'delete') return { table: tableId, key, before }
      const modified = mutation.modified as T
      if (modified.id !== key) throw new Error('Primary keys cannot change')
      let after = encode(modified)
      if (mutation.type === 'insert') {
        if (before) throw new Error(`Row already exists: ${tableId}/${key}`)
      } else {
        if (!before) throw new Error(`Row was deleted: ${tableId}/${key}`)
        const original = encode(mutation.original as T)
        // Apply only the caller's field changes to the latest authoritative row.
        const merged: Row = { ...before }
        for (const cell of new Set([...Object.keys(original), ...Object.keys(after)])) {
          if (own(original, cell) === own(after, cell) && Object.is(original[cell], after[cell])) continue
          if (own(after, cell)) merged[cell] = after[cell]!
          else delete merged[cell]
        }
        after = checkRow(merged)
      }
      decode(key, after)
      return { table: tableId, key, before, after }
    },
  }
  return {
    id: options.id ?? `tinybase:${tableId}`,
    getKey: item => item.id,
    startSync: true,
    gcTime: Infinity,
    sync: {
      rowUpdateMode: 'full',
      sync: ({ collection, begin, write, commit, markReady, markError }) => {
        if (claimed) throw new Error('Create fresh collection options for each collection')
        claimed = true
        let active = true
        let ready = false
        let known = new Map<string, Row>()
        const dirty = new Set<string>()
        const listeners: string[] = []
        const report = (error: unknown) => {
          if (!active) return
          syncError = asError(error)
          if (!ready) markError(syncError)
          try { options.onError?.(syncError) } catch { /* reporting must not break Store transactions */ }
        }
        const publish = (all: boolean) => {
          if (!active) return
          try {
            const keys = all ? new Set([...known.keys(), ...store.getRowIds(tableId)]) : new Set(dirty)
            dirty.clear()
            // Decode every changed row before opening a TanStack sync transaction.
            const changes = [...keys].flatMap(key => {
              const row = store.hasRow(tableId, key) ? store.getRow(tableId, key) : undefined
              if (equalRows(known.get(key), row)) return []
              return [{ key, row, item: row ? decode(key, row) : undefined }]
            })
            begin()
            for (const { key, item, row } of changes) {
              if (item) write({ type: known.has(key) ? 'update' : 'insert', value: item })
              else write({ type: 'delete', key })
              if (row) known.set(key, row)
              else known.delete(key)
            }
            const receipt = commit()
            syncError = undefined
            if (receipt !== true) void receipt.catch(report)
            if (!ready) { ready = true; markReady() }
          } catch (error) { report(error) }
        }
        const start = () => {
          if (!active) return
          listeners.push(store.addStartTransactionListener(() => { storeBusy = true }))
          listeners.push(store.addRowListener(tableId, null, (_s, _t, key) => { dirty.add(key) }))
          listeners.push(store.addDidFinishTransactionListener(() => {
            if (dirty.size) publish(syncError !== undefined)
            storeBusy = false
          }))
          resync = () => publish(true)
          bindings.set(collection, binding)
          publish(true)
        }
        if (options.ready) void options.ready.then(start).catch(report)
        else start()
        return () => {
          active = false
          listeners.forEach(id => store.delListener(id))
          bindings.delete(collection)
          resync = undefined
          claimed = false
          storeBusy = false
          syncError = undefined
          known.clear()
          dirty.clear()
        }
      },
    },
    onInsert: tinybaseMutationFn as NonNullable<CollectionConfig<T, string>['onInsert']>,
    onUpdate: tinybaseMutationFn as NonNullable<CollectionConfig<T, string>['onUpdate']>,
    onDelete: tinybaseMutationFn as NonNullable<CollectionConfig<T, string>['onDelete']>,
    utils: {
      getSyncError: () => syncError,
      resync: () => {
        if (!resync) throw new Error('Collection is not active')
        resync()
      },
    },
  }
}
