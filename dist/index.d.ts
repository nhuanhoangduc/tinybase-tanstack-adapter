import type { CollectionConfig, MutationFn, UtilsRecord } from '@tanstack/db';
import type { Row, Store } from 'tinybase';
export type Item = {
    id: string;
};
export interface RowCodec<T extends Item> {
    /** Pure, synchronous. Must preserve id and round-trip all stored cells. */
    decode(id: string, row: Row): T;
    /** Pure, synchronous. Return cells only; id lives in the TinyBase row key. */
    encode(item: T): Row;
}
export interface TinyBaseOptions<T extends Item> {
    store: Store;
    tableId: string;
    id?: string;
    codec?: RowCodec<T>;
    /** Optional synchronous domain validation, used for local AND remote rows. */
    validate?: (item: T) => void;
    /** Load your persister before initial sync. Rejections fail collection.preload(). */
    ready?: Promise<unknown>;
    /** Remote decoding/sync errors. Keep this callback non-throwing. */
    onError?: (error: Error) => void;
}
export interface TinyBaseUtils extends UtilsRecord {
    getSyncError(): Error | undefined;
    resync(): void;
}
/** Shared mutationFn for explicit TanStack transactions, including multiple tables.
 * All participating collections must use this adapter and the SAME TinyBase Store.
 * Resolves when accepted in memory, NOT when persisted to disk or synced remotely.
 */
export declare const tinybaseMutationFn: MutationFn;
/** One collection per TinyBase table. Eager in-memory sync; no SQLite query pushdown. */
export declare function tinybaseCollectionOptions<T extends Item>(options: TinyBaseOptions<T>): CollectionConfig<T, string, never, TinyBaseUtils> & {
    utils: TinyBaseUtils;
};
