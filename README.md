# TanStack DB ↔ TinyBase adapter

[![CI](https://github.com/nhuanhoangduc/tinybase-tanstack-adapter/actions/workflows/ci.yml/badge.svg)](https://github.com/nhuanhoangduc/tinybase-tanstack-adapter/actions/workflows/ci.yml) [MIT License](LICENSE)

A TanStack DB collection adapter backed by TinyBase Store or MergeableStore. Hướng dẫn tiếng Việt ở dưới.

Adapter TypeScript cho **TanStack DB 0.9.2** và **TinyBase 10.0.1**. TinyBase `Store`/`MergeableStore` giữ dữ liệu gốc; TanStack DB cung cấp collection, live query và mutation. Một collection tương ứng một TinyBase table. Không dùng TinyQL.

## Cài đặt

```sh
npm install github:nhuanhoangduc/tinybase-tanstack-adapter @tanstack/db@0.9.2 tinybase@10.0.1
```

Adapter chưa phát hành lên npm registry. Để phát triển từ mã nguồn:

```sh
git clone https://github.com/nhuanhoangduc/tinybase-tanstack-adapter.git
cd tinybase-tanstack-adapter
npm ci
npm run check
```

Bản này cố định hai phiên bản peer dependency trên; hãy chạy lại kiểm thử khi nâng phiên bản.

## Ví dụ cơ bản

```ts
import {createCollection, createLiveQueryCollection, eq} from '@tanstack/db'
import {createMergeableStore} from 'tinybase'
import {tinybaseCollectionOptions} from 'tinybase-tanstack-adapter'

type Task = {id: string; title: string; done: boolean; note?: string}
const store = createMergeableStore()

const tasks = createCollection(tinybaseCollectionOptions<Task>({
  store,
  tableId: 'tasks',
  validate(task) {
    if (typeof task.title !== 'string' || typeof task.done !== 'boolean') {
      throw new Error('Invalid task')
    }
  },
  onError(error) { console.error('TinyBase sync error', error) },
}))
await tasks.preload()

const pending = createLiveQueryCollection({
  query: q => q.from({task: tasks}).where(({task}) => eq(task.done, false)),
})
await pending.preload()

await tasks.insert({id: crypto.randomUUID(), title: 'Viết adapter', done: false}).isPersisted.promise
const firstId = [...tasks.keys()][0]!
await tasks.update(firstId, draft => { draft.done = true }).isPersisted.promise
await tasks.delete(firstId).isPersisted.promise

// Thay đổi nhận qua TinyBase synchronizer cũng cập nhật collection và live query.
store.setRow('tasks', 'remote-id', {title: 'Từ thiết bị khác', done: false})
console.log(pending.get('remote-id'))
```

Với React, dùng `useLiveQuery` từ `@tanstack/react-db` trên collection `tasks`. `createLiveQueryCollection` ở ví dụ chỉ minh họa việc query phản ứng mà không cần framework.

## Khởi động với persistence

Tạo và load TinyBase persister **trước khi preload** TanStack collection. `ready` cũng có thể là promise load để adapter chờ:

```ts
import {createMergeableStore} from 'tinybase'
import {createLocalPersister} from 'tinybase/persisters/persister-browser'
import {createCollection} from '@tanstack/db'
import {tinybaseCollectionOptions} from 'tinybase-tanstack-adapter'

const store = createMergeableStore()
const persister = createLocalPersister(store, 'my-app-v1')
const ready = (async () => {
  await persister.load()
  await persister.startAutoSave()
})()
const tasks = createCollection(tinybaseCollectionOptions<Task>({store, tableId: 'tasks', ready}))
await tasks.preload()
```

`LocalPersister` giữ được metadata CRDT nhưng chỉ hợp dữ liệu nhỏ vì giới hạn `localStorage`. Với dữ liệu lớn, chọn OPFS hoặc SQLite ở **JSON mode** của TinyBase. `IndexedDbPersister` của TinyBase 10 không hỗ trợ `MergeableStore`. Một `MergeableStore` cần được persist cả metadata, không chỉ các row.

Kết nối WebSocket synchronizer của TinyBase vào cùng `store` sau khi load persistence; mọi thay đổi từ đồng bộ sẽ đến collection qua listener. Adapter không tạo server WebSocket, quản lý xác thực, retry mạng hay persister thay cho ứng dụng.

## Giao dịch nhiều bảng

```ts
import {createTransaction} from '@tanstack/db'
import {tinybaseMutationFn} from 'tinybase-tanstack-adapter'

// projects và tasks là hai collection của CÙNG một TinyBase Store.
const tx = createTransaction({mutationFn: tinybaseMutationFn})
tx.mutate(() => {
  projects.insert({id: 'p1', name: 'Dự án'})
  tasks.insert({id: 't1', projectId: 'p1', title: 'Công việc', done: false})
})
await tx.isPersisted.promise
```

Các thao tác trong batch được tiền kiểm rồi ghi trong một TinyBase transaction. Nếu TinyBase schema từ chối hoặc thay đổi cell, batch được rollback. Giao dịch khác store bị từ chối. Không ghi vào collection từ bên trong TinyBase transaction/listener.

## Giới hạn và cách xử lý lỗi

- `isPersisted.promise` xác nhận ghi vào **TinyBase trong bộ nhớ**. Nó không xác nhận đã lưu xuống ổ hay gửi đến peer. Theo dõi persister/synchronizer riêng nếu UI cần trạng thái đó.
- Mặc định mỗi row phải có `id: string` riêng, tối thiểu một cell, và cell là `string | number | boolean` hữu hạn. `undefined` ở field tùy chọn được bỏ qua. Dùng `codec` để serialize `Date`, array, object, `null`; codec phải round-trip đúng từng cell.
- Adapter nạp toàn bộ table vào TanStack DB và cập nhật theo row listener. Không có query pushdown xuống SQLite hay partial sync; dữ liệu lớn cần chiến lược khác.
- Khi update, adapter áp dụng chỉ các field mà người dùng sửa lên row TinyBase mới nhất. Nếu một peer đồng thời sửa **cùng field**, mutation local được áp dụng sau khi commit; TinyBase sẽ quyết định trạng thái khi merge tiếp.
- TinyBase `setPartialRow`/`setCell` có thể âm thầm bỏ qua dữ liệu sai schema. Adapter kiểm tra kết quả sau transaction và rollback. `validate` giúp kiểm tra quy tắc nghiệp vụ trước đó.
- Row từ peer không hợp lệ sẽ không được đẩy vào collection. `onError` và `collection.utils.getSyncError()` báo lỗi; sửa row gốc hoặc gọi `collection.utils.resync()` sau khi đã khắc phục. Snapshot cuối cùng hợp lệ vẫn hiển thị.
- Trong TanStack DB 0.9.2, xóa field tùy chọn có thể để `undefined` trong object optimistic một lúc; TinyBase row đã xóa cell và `collection.get(id)?.field` trả về `undefined`.
- Mỗi table cần một collection đang hoạt động, với một lời gọi `tinybaseCollectionOptions` riêng. Gọi `collection.cleanup()` khi không dùng nữa.

Nguồn API: [TanStack custom collection](https://tanstack.com/db/latest/docs/guides/collection-options-creator), [TinyBase Store](https://tinybase.org/api/store/interfaces/store/store/), [TinyBase persistence](https://tinybase.org/api/persisters/), [TinyBase synchronizer](https://tinybase.org/guides/synchronization/using-a-synchronizer/).
