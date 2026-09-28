import { afterEach, describe, expect, it } from 'vitest'
import { createCollection, createTransaction, createLiveQueryCollection, eq } from '@tanstack/db'
import { createMergeableStore, createStore } from 'tinybase'
import { createFilePersister } from 'tinybase/persisters/persister-file'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { tinybaseCollectionOptions, tinybaseMutationFn } from '../src/index.js'
import type { TinyBaseOptions } from '../src/index.js'

type Task = { id: string; title: string; done: boolean; note?: string; priority?: number }
const cleanups: (() => unknown)[] = []
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup() })
function setup(options: Partial<TinyBaseOptions<Task>> = {}) {
  const store = options.store ?? createMergeableStore()
  const collection = createCollection(tinybaseCollectionOptions<Task>({store, tableId: 'tasks', ...options}))
  cleanups.push(() => collection.cleanup())
  return {store, collection}
}
async function persisted(tx: {isPersisted: {promise: Promise<unknown>}}) { await tx.isPersisted.promise }
const row = (title = 'First') => ({title, done: false})

describe('TinyBase collection adapter', () => {
  it('hydrates existing rows and an empty store', async () => {
    const store = createMergeableStore().setRow('tasks', 'a', row())
    const {collection} = setup({store})
    await collection.preload()
    expect(collection.get('a')).toMatchObject({id: 'a', ...row()})
    const empty = setup()
    await empty.collection.preload()
    expect(empty.collection.size).toBe(0)
  })
  it('inserts, patches, deletes and propagates direct Store changes', async () => {
    const {store, collection} = setup()
    await collection.preload()
    await persisted(collection.insert({id: 'a', ...row()}))
    expect(store.getRow('tasks', 'a')).toEqual(row())
    await persisted(collection.update('a', draft => { draft.done = true; draft.note = 'hello' }))
    expect(store.getRow('tasks', 'a')).toEqual({...row(), done: true, note: 'hello'})
    await persisted(collection.update('a', draft => { delete draft.note }))
    expect(collection.get('a')?.note).toBeUndefined()
    expect(store.hasCell('tasks', 'a', 'note')).toBe(false)
    store.setCell('tasks', 'a', 'title', 'remote')
    expect(collection.get('a')?.title).toBe('remote')
    store.delCell('tasks', 'a', 'done')
    expect(collection.get('a')).not.toHaveProperty('done')
    await persisted(collection.delete('a'))
    expect(store.hasRow('tasks', 'a')).toBe(false)
    expect(collection.has('a')).toBe(false)
  })
  it('handles transaction batches, table deletion and store replacement', async () => {
    const {store, collection} = setup()
    await collection.preload()
    store.transaction(() => {
      store.setRow('tasks', 'a', row('A'))
      store.setRow('tasks', 'b', row('B'))
    })
    expect(collection.size).toBe(2)
    store.setTables({tasks: {c: row('C')}})
    expect([...collection.keys()]).toEqual(['c'])
    store.delTable('tasks')
    expect(collection.size).toBe(0)
  })
  it('does not publish rolled back TinyBase transactions', async () => {
    const {store, collection} = setup()
    await collection.preload()
    store.transaction(() => store.setRow('tasks', 'a', row()), () => true)
    expect(collection.size).toBe(0)
  })
  it('supports bulk insert and mixed explicit transactions', async () => {
    const {store, collection} = setup()
    await collection.preload()
    await persisted(collection.insert([{id:'a', ...row()}, {id:'b', ...row()}]))
    const tx = createTransaction({mutationFn: tinybaseMutationFn})
    tx.mutate(() => {
      collection.update('a', d => {d.done = true})
      collection.delete('b')
      collection.insert({id:'c', ...row()})
    })
    await persisted(tx)
    expect(store.getRowIds('tasks').sort()).toEqual(['a','c'])
    expect(collection.get('a')?.done).toBe(true)
  })
  it('commits multiple tables atomically in one Store transaction', async () => {
    const {store, collection} = setup()
    const other = setup({store, tableId: 'other', id: 'other'}).collection
    await Promise.all([collection.preload(), other.preload()])
    let calls = 0
    const listener = store.addDidFinishTransactionListener(s => {if(s.getTransactionLog()[0]) calls++})
    const tx = createTransaction({mutationFn: tinybaseMutationFn})
    tx.mutate(() => {collection.insert({id:'a', ...row()}); other.insert({id:'b', ...row()})})
    await persisted(tx)
    store.delListener(listener)
    expect(calls).toBe(1)
    expect(other.get('b')).toMatchObject({id:'b', ...row()})
  })
  it('rejects transactions spanning different Stores without partial writes', async () => {
    const a = setup(), b = setup({id:'other'})
    await Promise.all([a.collection.preload(), b.collection.preload()])
    const tx = createTransaction({mutationFn: tinybaseMutationFn})
    tx.mutate(() => {a.collection.insert({id:'a', ...row()}); b.collection.insert({id:'b', ...row()})})
    await expect(tx.isPersisted.promise).rejects.toThrow('same TinyBase Store')
    expect(a.store.hasTable('tasks')).toBe(false)
    expect(b.store.hasTable('tasks')).toBe(false)
  })
  it('rolls back the complete batch on TinyBase schema rejection', async () => {
    const store = createMergeableStore().setTablesSchema({tasks: {title: {type:'string'}, done: {type:'boolean'}}})
    const {collection} = setup({store})
    await collection.preload()
    const tx = collection.insert([{id:'a', ...row()}, {id:'b', ...row(), priority: 1}])
    await expect(tx.isPersisted.promise).rejects.toThrow('rejected or transformed')
    expect(store.getTable('tasks')).toEqual({})
    expect(collection.size).toBe(0)
  })
  it('prevalidates unsupported cells and empty rows', async () => {
    const {store, collection} = setup()
    await collection.preload()
    const tx = collection.insert({id:'bad', ...row(), note: null} as unknown as Task)
    await expect(tx.isPersisted.promise).rejects.toThrow('Invalid cell')
    expect(store.getTable('tasks')).toEqual({})
    await expect(collection.insert({id:'empty'} as Task).isPersisted.promise).rejects.toThrow('empty row')
  })
  it('preserves concurrent changes to unrelated cells', async () => {
    const {store, collection} = setup()
    await collection.preload()
    await persisted(collection.insert({id:'a', ...row()}))
    const tx = createTransaction({autoCommit:false, mutationFn: tinybaseMutationFn})
    tx.mutate(() => collection.update('a', d => {d.done = true}))
    store.setCell('tasks', 'a', 'title', 'Remote changed title')
    await tx.commit()
    expect(store.getRow('tasks', 'a')).toEqual({title:'Remote changed title', done:true})
    expect(collection.get('a')).toMatchObject({id:'a', title:'Remote changed title', done:true})
  })
  it('does not resurrect a remotely deleted row', async () => {
    const {store, collection} = setup()
    await collection.preload()
    await persisted(collection.insert({id:'a', ...row()}))
    const tx = createTransaction({autoCommit:false, mutationFn: tinybaseMutationFn})
    tx.mutate(() => collection.update('a', d => {d.done = true}))
    store.delRow('tasks', 'a')
    await expect(tx.commit()).rejects.toThrow('deleted')
    expect(collection.has('a')).toBe(false)
  })
  it('receives CRDT merge changes from another MergeableStore', async () => {
    const a = createMergeableStore(), b = createMergeableStore()
    const {collection} = setup({store:a})
    await collection.preload()
    b.setRow('tasks', 'a', row('Peer'))
    a.merge(b)
    expect(collection.get('a')?.title).toBe('Peer')
    await persisted(collection.update('a', d => {d.done = true}))
    b.merge(a)
    expect(b.getCell('tasks', 'a', 'done')).toBe(true)
    b.delRow('tasks', 'a')
    a.merge(b)
    expect(collection.has('a')).toBe(false)
  })
  it('waits for persistence hydration and reports initialization errors', async () => {
    let loaded!: () => void
    const ready = new Promise<void>(resolve => {loaded = resolve})
    const {store, collection} = setup({ready})
    const loading = collection.preload()
    store.setRow('tasks', 'a', row('Disk'))
    loaded()
    await loading
    expect(collection.get('a')?.title).toBe('Disk')
    const failed = setup({ready: Promise.reject(new Error('Disk unavailable'))})
    await expect(failed.collection.preload()).rejects.toThrow('Disk unavailable')
  })
  it('round-trips CRDT data through a real TinyBase file persister', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'tinybase-tanstack-'))
    try {
      const path = join(dir, 'data.json')
      const a = createMergeableStore()
      const {collection: first} = setup({store:a})
      await first.preload()
      await persisted(first.insert({id:'a', ...row('Saved')}))
      const writer = createFilePersister(a, path)
      await writer.save()
      await writer.destroy()

      const b = createMergeableStore()
      const reader = createFilePersister(b, path)
      const {collection: second} = setup({store:b, id:'restored', ready:reader.load()})
      await second.preload()
      expect(second.get('a')?.title).toBe('Saved')
      const peer = createMergeableStore().setRow('tasks','peer',row('Peer'))
      b.merge(peer)
      expect(second.get('peer')?.title).toBe('Peer')
      await reader.destroy()
    } finally {
      await rm(dir, {recursive:true, force:true})
    }
  })
  it('does not attach listeners if disposed during hydration', async () => {
    let loaded!: () => void
    const ready = new Promise<void>(resolve => {loaded = resolve})
    const {store, collection} = setup({ready})
    await collection.cleanup()
    loaded()
    await Promise.resolve()
    expect(store.getListenerStats().row).toBe(0)
  })
  it('removes all owned listeners on cleanup', async () => {
    const {store, collection} = setup()
    await collection.preload()
    expect(store.getListenerStats().row).toBe(1)
    await collection.cleanup()
    expect(store.getListenerStats().row).toBe(0)
    expect(store.getListenerStats().transaction).toBe(0)
  })
  it('works with ordinary Store as well as MergeableStore', async () => {
    const {store, collection} = setup({store:createStore()})
    await collection.preload()
    await persisted(collection.insert({id:'a', ...row()}))
    expect(store.getRow('tasks','a')).toEqual(row())
  })
  it('supports custom codecs for rich types', async () => {
    type Event = {id:string; at:Date; tags:string[]}
    const store = createMergeableStore()
    const events = createCollection(tinybaseCollectionOptions<Event>({store, tableId:'events', codec:{
      encode: e => ({at:e.at.toISOString(), tags:JSON.stringify(e.tags)}),
      decode: (id,r) => ({id, at:new Date(r.at as string), tags:JSON.parse(r.tags as string)}),
    }}))
    cleanups.push(() => events.cleanup())
    await events.preload()
    await persisted(events.insert({id:'a', at:new Date('2026-01-01'), tags:['x']}))
    expect(events.get('a')?.at).toBeInstanceOf(Date)
    expect(store.getCell('events','a','tags')).toBe('["x"]')
  })
  it('reports invalid remote rows, retains the last good snapshot, then recovers', async () => {
    const errors: Error[] = []
    const {store, collection} = setup({validate: t => {if(typeof t.title !== 'string') throw new Error('title required')}, onError:e => errors.push(e)})
    await collection.preload()
    store.setRow('tasks','a',row())
    store.delCell('tasks','a','title')
    expect(errors).toHaveLength(1)
    expect(collection.get('a')?.title).toBe('First')
    expect(collection.utils.getSyncError()?.message).toBe('title required')
    store.setCell('tasks','a','title','Fixed')
    expect(collection.get('a')?.title).toBe('Fixed')
    expect(collection.utils.getSyncError()).toBeUndefined()
  })
  it('feeds reactive TanStack queries without TinyQL', async () => {
    const {store, collection} = setup()
    await collection.preload()
    const query = createLiveQueryCollection({query: q => q.from({task:collection}).where(({task}) => eq(task.done,false))})
    cleanups.push(() => query.cleanup())
    await query.preload()
    store.setRow('tasks','a',row())
    expect(query.size).toBe(1)
    await persisted(collection.update('a',d => {d.done=true}))
    expect(query.size).toBe(0)
  })
})
