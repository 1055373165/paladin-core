# Day 1：KV Store — 最简陋的配置中心

> 今天的目标：用 BoltDB 实现一个带 revision 语义的 KV 存储。
> 代码量：~200 行 | 新增文件：`store/store.go`、`store/bolt.go`、`cmd/paladin-core/main.go`

---

## 1. 从一个问题开始

假设你有 1000 个微服务，每个都有数据库地址、限流阈值、功能开关等配置。你不可能每次改一个参数就重新发版上线。

最朴素的解决方案：搞一个中心化的 KV 存储，所有服务启动时来这里读配置。

```
Service A  ──GET /config/db_host──→  [ 配置中心 ]  ←── 运维人员修改配置
Service B  ──GET /config/db_host──→  [ 配置中心 ]
```

今天我们就实现这个 KV 存储——配置中心的地基。

---

## 2. 为什么需要 revision？

你可能会想：用一个 `map[string][]byte` 不就行了？

不行。因为配置中心不只是"存取键值"，还需要回答一个关键问题：**这个配置是什么时候变的？**

比如，SDK 需要知道"自从我上次查询到现在，有哪些配置变了"。如果没有版本号，SDK 只能每次全量拉取所有配置做 diff——这在配置项多的时候非常浪费。

引入 `revision`（全局单调递增计数器），每次写操作 revision++：

```
put db_host=10.0.0.1   → revision=1
put db_port=3306        → revision=2
put db_host=10.0.0.2   → revision=3  (修改 db_host)
delete db_port          → revision=4
```

SDK 说"给我 revision > 2 之后的所有变更"，配置中心就能精确返回 `[db_host 改成了 10.0.0.2, db_port 被删了]`。

### 面试灵魂拷问

> **Q：为什么用 revision 而不是 timestamp？**
>
> A：分布式系统中物理时钟不可靠——NTP 可能漂移、闰秒可能导致时间回退。revision 是逻辑时钟（Lamport Clock 的简化版），由单一写入者（Leader）递增，保证因果关系：如果 A 发生在 B 之前，`revision(A) < revision(B)` 一定成立。timestamp 做不到这个保证。

---

## 3. Entry 的四个版本字段

一条配置项（Entry）不只有一个版本号，而是有四个：

```go
type Entry struct {
    Key            string  // 配置的 key
    Value          []byte  // 配置的值
    Revision       uint64  // 当前全局版本号
    CreateRevision uint64  // 这个 key 被首次创建时的全局版本号
    ModRevision    uint64  // 这个 key 最后一次被修改时的全局版本号
    Version        int64   // 这个 key 被修改了多少次（从 1 开始）
}
```

为什么要分这么细？看一个例子：

```
put app/db_host=10.0.0.1   → Entry{Revision:1, CreateRevision:1, ModRevision:1, Version:1}
put app/db_port=3306        → (db_host 不受影响)
put app/db_host=10.0.0.2   → Entry{Revision:3, CreateRevision:1, ModRevision:3, Version:2}
```

- `CreateRevision=1` 告诉你：db_host 是在全局第 1 次操作时创建的
- `ModRevision=3` 告诉你：db_host 最后被修改是全局第 3 次操作
- `Version=2` 告诉你：db_host 本身被改了 2 次

这四个字段是 etcd 的设计，Paladin 也继承了这个设计。后续在 Watch 机制（Day 3）中你会看到为什么需要这些字段。

---

## 4. 为什么选 BoltDB？

| 可选方案 | 优点 | 缺点 |
|---------|------|------|
| 内存 map | 最快 | 进程重启数据丢失 |
| SQLite | 功能全 | 配置中心不需要关系模型 |
| BoltDB | 纯 Go、嵌入式、事务级一致性 | 单写者模型（同时只能有一个写事务） |
| BadgerDB | LSM-tree，写入性能更高 | 配置中心写入频率低，用不上 |

BoltDB 的单写者模型看起来是缺点，但实际上**和 Raft 完美对齐**——Raft 保证只有 Leader 写入，所以单写者模型不是瓶颈，反而省去了写冲突处理的复杂性。

---

## 5. 核心实现

### 5.1 Store 接口

```go
// store/store.go
type Store interface {
    Put(key string, value []byte) (*PutResult, error)
    Get(key string) (*Entry, error)
    Delete(key string) (*Entry, error)
    List(prefix string) ([]*Entry, error)
    Rev() uint64
    Close() error
}
```

接口设计的一个关键点：`Put` 返回 `PutResult`，里面包含 `PrevEntry`（修改前的旧值）。这是为 Watch 的"变更事件"做准备——事件需要同时包含新旧值。

### 5.2 BoltDB 实现

BoltDB 的数据组织用两个 bucket：

```
bucketData: 存配置数据    key → JSON(Entry)
bucketMeta: 存元信息      "rev" → uint64(当前全局 revision)
```

`Put` 的核心逻辑在一个 BoltDB 事务内完成：

```go
func (s *BoltStore) Put(key string, value []byte) (*PutResult, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    err := s.db.Update(func(tx *bolt.Tx) error {
        // 1. 读取旧值（如果有）
        prev := readEntry(tx, key)

        // 2. 全局 revision++
        newRev := s.rev + 1

        // 3. 构造新 Entry
        entry := &Entry{
            Key: key, Value: value, Revision: newRev,
        }
        if prev != nil {
            entry.CreateRevision = prev.CreateRevision  // 保持不变
            entry.Version = prev.Version + 1            // 递增
        } else {
            entry.CreateRevision = newRev               // 首次创建
            entry.Version = 1
        }
        entry.ModRevision = newRev

        // 4. 写入 BoltDB
        data.Put([]byte(key), marshal(entry))
        meta.Put(keyRev, encode(newRev))

        s.rev = newRev  // 更新内存缓存
        return nil
    })  // BoltDB 事务自动保证原子性
    // ...
}
```

**关键点：** revision 的递增和数据的写入在**同一个 BoltDB 事务**中完成。如果中途崩溃，要么都写入，要么都不写入——这就是事务级一致性。

### 5.3 Delete 也要 bump revision

容易被忽略的细节：删除操作也要让全局 revision 递增。

```go
func (s *BoltStore) Delete(key string) (*Entry, error) {
    // ...
    newRev := s.rev + 1    // revision 递增！
    data.Delete([]byte(key))
    meta.Put(keyRev, encode(newRev))
    // ...
}
```

为什么？因为 Watch 需要看到删除事件。如果删除不 bump revision，一个 SDK 在 `revision=5` 时开始 Watch，中间有个 key 被删了但 revision 没变，SDK 就会永远不知道这个 key 被删了。

### 5.4 Revision 的持久化与恢复

```go
func NewBoltStore(path string) (*BoltStore, error) {
    // ...
    // 启动时从磁盘恢复 revision
    var rev uint64
    db.View(func(tx *bolt.Tx) error {
        if v := tx.Bucket(bucketMeta).Get(keyRev); v != nil {
            rev = binary.BigEndian.Uint64(v)
        }
        return nil
    })
    return &BoltStore{db: db, rev: rev}, nil
}
```

这保证进程重启后 revision 从上次的位置继续，而不是从 0 开始。这一点在测试 `TestRevisionPersistence` 中验证了。

---

## 6. 验证

### 运行测试

```bash
cd paladin-core
go test ./store/ -v
```

预期输出：7/7 PASS

```
--- PASS: TestPutAndGet
--- PASS: TestGetNotFound
--- PASS: TestDelete
--- PASS: TestDeleteNotFound
--- PASS: TestListPrefix
--- PASS: TestRevisionMonotonic
--- PASS: TestRevisionPersistence
```

### CLI 体验

```bash
go run ./cmd/paladin-core put app/db_host 10.0.0.1
# OK  rev=1  version=1  key=app/db_host

go run ./cmd/paladin-core put app/db_port 3306
# OK  rev=2  version=1  key=app/db_port

go run ./cmd/paladin-core put app/db_host 10.0.0.2
# OK  rev=3  version=2  key=app/db_host
#     prev_value=10.0.0.1  prev_rev=1

go run ./cmd/paladin-core list app/
#   app/db_host    = 10.0.0.2   rev=3  ver=2
#   app/db_port    = 3306       rev=2  ver=1
```

---

## 7. 今天的收获

| 你学到了什么 | 面试中怎么说 |
|------------|-----------|
| revision 是逻辑时钟 | "分布式系统中物理时钟不可靠，revision 作为 Lamport Clock 保证因果序" |
| BoltDB 单写者模型 | "和 Raft Leader-only-write 天然对齐，不需要额外的写冲突处理" |
| 事务级原子性 | "revision 递增和数据写入在同一个 BoltDB 事务中，崩溃安全" |
| Delete 也要 bump revision | "Watch 需要看到删除事件，否则客户端感知不到配置被删" |

---

## 8. 明天预告：Day 2 — HTTP API + 多租户

今天的 Store 只能通过 CLI 访问。明天我们会在上面加 HTTP API，让任何语言的客户端都能远程操作配置。同时引入 `tenant/namespace/name` 三级结构，为多环境配置管理做准备。
