# Day 4：Raft 共识 — 从单机到集群

> 今天的目标：引入 HashiCorp Raft，实现 FSM 接口，所有写操作经 Raft 共识后应用。
> 代码量：~350 行 | 新增：`raft/node.go`、`raft/node_test.go`

---

## 1. 为什么需要 Raft？

Day 1-3 实现了一个功能完整的**单机**配置中心。但单机 = 单点故障。进程挂了，所有服务拿不到配置。

Raft 把单机变成多节点集群：3 个节点中任意 1 个挂掉，系统仍然可用。

```
    ┌─────────┐
    │ Leader  │  ← 所有写操作在这里
    │ Node 1  │
    └────┬────┘
         │ AppendEntries RPC
    ┌────┴────┐    ┌─────────┐
    │Follower │    │Follower │
    │ Node 2  │    │ Node 3  │
    └─────────┘    └─────────┘
```

## 2. Raft 写流程（你在面试中需要手画的）

```
Client → Leader
  1. 构造 Op{type:"put", key:"db_host", value:"10.0.0.1"}
  2. 序列化为 JSON，调用 raft.Apply(data)
  3. Leader 追加到本地 log
  4. 通过 AppendEntries RPC 并行发给所有 Follower
  5. 收到 ≥ quorum (n/2+1) 个 ACK → entry 标记为 committed
  6. 两件事并行：
     a. 返回成功给 Client
     b. Apply 到 FSM → BoltStore.Put → WatchCache.Append
  7. Follower 通过后续 AppendEntries RPC 中的 commitIndex 字段
     知道该 entry 已 committed，也执行 FSM.Apply
```

## 3. FSM 接口

HashiCorp Raft 要求实现三个方法：

```go
type FSM interface {
    Apply(*raft.Log) interface{}       // 应用已提交的 log entry
    Snapshot() (FSMSnapshot, error)    // 创建快照（log 压缩）
    Restore(io.ReadCloser) error      // 从快照恢复
}
```

我们的实现：

```go
func (f *FSM) Apply(log *raft.Log) interface{} {
    var op Op
    json.Unmarshal(log.Data, &op)  // 反序列化操作

    switch op.Type {
    case "put":
        result, _ := f.store.Put(op.Key, op.Value)  // 写入 BoltDB + 触发 Watch
        return &OpResult{Entry: result.Entry}
    case "delete":
        deleted, _ := f.store.Delete(op.Key)
        return &OpResult{Entry: deleted}
    }
}
```

### 面试灵魂拷问

> **Q：FSM.Apply 里能不能做网络调用？**
>
> A：绝对不能。Apply 在 Raft 的 applyCh goroutine 里串行执行。任何阻塞（网络 IO、远程数据库调用）都会卡住整个状态机，导致后续 log 无法 apply，最终触发选举超时。这就是为什么 Paladin 用 BoltDB（本地磁盘）而不是远程数据库。

## 4. Snapshot：为什么需要？

Raft log 不能无限增长。Snapshot 把当前状态全量导出，然后截断已 apply 的 log。

新节点加入集群时如果 log 落后太多（老 log 已被截断），Leader 发送 Snapshot → 新节点 Restore → 从 Snapshot 之后的 log 继续追赶。

我们的 Snapshot 实现很简单：将所有 Entry JSON 序列化。Paladin 原版用 BoltDB 的原生 Snapshot（二进制导出整个数据库文件）。

## 5. 写操作路由

```go
func (n *Node) Apply(op Op, timeout time.Duration) (*OpResult, error) {
    if n.raft.State() != raft.Leader {
        return nil, ErrNotLeader  // Day 5 会加自动转发
    }
    data, _ := json.Marshal(op)
    future := n.raft.Apply(data, timeout)
    return future.Response().(*OpResult), future.Error()
}
```

当前版本：非 Leader 直接返回错误。Day 5 会加 ForwardRPC，让 Follower 透明转发给 Leader。

## 6. 读操作：允许脏读

```go
func (n *Node) Get(key string) (*store.Entry, error) {
    return n.store.Get(key)  // 直接读本地 BoltDB
}
```

Follower 的数据可能落后 Leader 几个 log entry。对配置中心来说这通常可以接受（配置不是每秒都在变）。Day 5 会加 `VerifyLeader()` 选项支持强一致读。

## 7. 验证

5 个 Raft 测试全部通过：
- 单节点 Bootstrap → 自动选为 Leader
- Put/Get 通过 Raft 共识
- Delete 通过 Raft 共识
- Revision 单调递增
- Watch 集成：Raft Apply → FSM → WatchCache → 事件可查
- 非 Leader 写入返回 ErrNotLeader

## 8. 明天预告：Day 5 — ForwardRPC + 一致性读

当前客户端必须知道谁是 Leader 才能发写请求。明天实现 ForwardRPC——Follower 自动把写请求转发给 Leader，客户端连任意节点即可。
