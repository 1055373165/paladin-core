# Day 3：Watch 机制 — 从拉变推

> 今天的目标：实现 WatchCache 环形缓冲区 + HTTP 长轮询，让客户端实时感知配置变更。
> 代码量：~300 行 | 新增文件：`store/watch.go`、`store/watchable.go`、`server/watch.go` 及测试

---

## 1. 为什么需要 Watch？

Day 2 的 HTTP API 是**拉模型**（Pull）：客户端必须反复轮询才知道配置变了没。

```
// 朴素轮询 — 99.9% 的请求返回"没变化"
for {
    config := GET("/api/v1/config/public/prod/db_host")
    if config != lastConfig { handleChange(config) }
    sleep(1s)
}
```

两个问题：
1. **延迟高**：1 秒轮询间隔 = 最多 1 秒才能发现变更
2. **浪费资源**：绝大多数请求返回"没变化"，白白消耗带宽和服务端 CPU

解决方案是**长轮询**（Long Polling）——客户端发请求后阻塞等待，服务端有变更时立刻返回：

```
// 长轮询 — 有变更才返回
for {
    events := GET("/api/v1/watch/public/prod/?revision=N&timeout=30")
    // ↑ 服务端在 30 秒内有变更就立刻返回，否则超时返回空
    if len(events) > 0 { handleChanges(events); N = latestRev }
}
```

延迟从"轮询间隔"降到"网络 RTT"（通常 < 1ms 内网）。这正是 Paladin SDK V2 的核心机制。

---

## 2. 核心数据结构：环形缓冲区（Ring Buffer）

Watch 的服务端需要缓存最近的变更事件，供长轮询查询。用什么数据结构？

| 方案 | 追加 | 查 "rev > N" 的事件 | 内存 | 适合 Watch？ |
|------|------|---------------------|------|------------|
| 无界 slice | O(1) | O(n) 遍历 | 无上限 ❌ | 内存泄漏风险 |
| Channel | O(1) | **不支持** ❌ | 固定 | 只能 FIFO，不能按 revision 查询 |
| 链表 | O(1) | O(n) 遍历 | 无上限 ❌ | 同 slice |
| **环形缓冲区** | **O(1)** | **遍历有效区间** | **固定** ✅ | ✅ |

环形缓冲区的关键：**固定容量**，新事件覆盖最老的事件，内存永远不会增长。

### 工作原理

```
容量=5 的环形缓冲区，已写入 8 个事件（count=8）：

数组位置:  [0]  [1]  [2]  [3]  [4]
存储事件:  r6   r7   r8   r4   r5
                      ↑          ↑
                    newest     oldest

oldest = count - capacity = 8 - 5 = 3  → pos = 3 % 5 = 3
newest = count - 1 = 7                  → pos = 7 % 5 = 2

事件 r1, r2, r3 已被覆盖（丢失）
```

追加操作：
```go
func (wc *WatchCache) Append(event Event) {
    pos := wc.count % wc.capacity  // 计算写入位置
    wc.buf[pos] = event             // 覆盖最老的
    wc.count++                      // 总计数递增（单调不减）
    wc.cond.Broadcast()             // 唤醒所有等待者
}
```

查询操作（"给我 revision > N 之后的事件"）：
```go
func (wc *WatchCache) getEventsLocked(afterRev uint64, prefix string) []Event {
    oldest := max(0, wc.count - wc.capacity)  // 最老的有效事件
    var result []Event
    for i := oldest; i < wc.count; i++ {
        pos := i % wc.capacity
        ev := wc.buf[pos]
        if ev.Entry.Revision > afterRev && strings.HasPrefix(ev.Entry.Key, prefix) {
            result = append(result, ev)
        }
    }
    return result
}
```

Paladin 原版用 **10 万条**容量。我们默认用 4096——配置变更频率低，足够了。

---

## 3. sync.Cond：阻塞与唤醒的艺术

长轮询的关键问题：客户端请求"给我 revision > 5 之后的事件"，但当前还没有。怎么高效等待？

**错误做法**——轮询检查：
```go
for {
    events := getEvents(afterRev)
    if len(events) > 0 { return events }
    time.Sleep(10ms)  // 浪费 CPU！且延迟至少 10ms
}
```

**正确做法**——条件变量 `sync.Cond`：
```go
wc.cond.Wait()  // 释放锁，阻塞当前 goroutine，零 CPU 开销
                // 被 Broadcast() 唤醒后，重新获取锁，继续执行
```

完整的等待逻辑：
```go
func (wc *WatchCache) WaitForEvents(afterRev uint64, prefix string, timeout time.Duration) []Event {
    deadline := time.Now().Add(timeout)
    wc.mu.Lock()
    defer wc.mu.Unlock()

    for {
        // 1. 检查有没有匹配的事件
        events := wc.getEventsLocked(afterRev, prefix)
        if len(events) > 0 { return events }

        // 2. 超时？
        if time.Now().After(deadline) { return nil }

        // 3. 没有事件，阻塞等待
        //    当 Append() 调用 Broadcast() 时，所有等待者被唤醒
        wc.cond.Wait()
    }
}
```

**重点**：`cond.Wait()` 做了三件事——① 释放 `mu` 锁 ② 挂起当前 goroutine ③ 被唤醒后重新获取 `mu` 锁。这保证了唤醒后可以安全地再次读取缓冲区。

### 面试灵魂拷问 #1

> **Q：sync.Cond 的 Wait 为什么必须在 for 循环里调用？**
>
> A：因为存在**虚假唤醒**（spurious wakeup）。Broadcast 唤醒所有 goroutine，但可能某个 goroutine 被唤醒后发现事件不匹配它的 prefix，需要继续等待。所以 Wait 之后必须重新检查条件，不能假设条件一定满足。

---

## 4. WatchableStore：存储与事件的桥梁

Watch 事件不能凭空产生——必须和存储操作绑定。`WatchableStore` 用 Go 的组合（embedding）把 BoltStore 和 WatchCache 粘合在一起：

```go
type WatchableStore struct {
    *BoltStore          // Day 1 的存储（Go 组合，继承所有方法）
    wc *WatchCache      // Day 3 的事件缓存
}

// 重写 Put — 先写存储，再追加事件
func (ws *WatchableStore) Put(key string, value []byte) (*PutResult, error) {
    result, err := ws.BoltStore.Put(key, value)  // ① 写 BoltDB
    if err != nil { return nil, err }

    ws.wc.Append(Event{                          // ② 追加 Watch 事件
        Type:      EventPut,
        Entry:     result.Entry,
        PrevEntry: result.PrevEntry,              // 旧值！Watch 需要
    })
    return result, nil
}
```

**设计细节**：事件包含 `PrevEntry`（修改前的旧值）。Watch 的消费者可以做差异比较——"db_host 从 10.0.0.1 改成了 10.0.0.2"，而不只是"db_host 变了"。

**在 Day 4（Raft）之后**，链路变为：
```
Client → Raft Apply → FSM.Apply → WatchableStore.Put → BoltDB + WatchCache
```
WatchableStore 的接口完全不变——这就是接口抽象的价值。

---

## 5. HTTP Watch 端点

```
GET /api/v1/watch/{tenant}/{namespace}/?revision=N&timeout=30
```

完整交互流程：

```
Client:  GET /api/v1/watch/public/prod/?revision=5&timeout=30
Server:  解析 prefix="public/prod/", afterRev=5, timeout=30s
         → WatchCache.WaitForEvents(5, "public/prod/", 30s)
         → 阻塞等待...

运维人员: PUT /api/v1/config/public/prod/db_host  body="10.0.0.2"
Server:  BoltStore.Put → revision=6
         WatchCache.Append(Event{rev=6, key="public/prod/db_host"})
         cond.Broadcast() → 唤醒所有等待者

Client:  收到响应 (延迟 ≈ 网络 RTT)
```

响应格式：
```json
{
  "revision": 6,
  "events": [
    {
      "type": "PUT",
      "entry": {"key": "public/prod/db_host", "value": "10.0.0.2", "revision": 6},
      "prev_entry": {"key": "public/prod/db_host", "value": "10.0.0.1", "revision": 3}
    }
  ]
}
```

客户端收到后用 `revision=6` 发起下一轮 Watch，形成持续的事件流。

---

## 6. 环形缓冲区溢出怎么办？

如果客户端离线太久（比如重启了 10 分钟），它上次的 revision 对应的事件可能已经被覆盖了。

两种策略：

| 策略 | 做法 | 使用者 |
|------|------|--------|
| 返回 `ErrCompacted` | 告诉客户端"你要的事件已经丢了，请全量重拉" | etcd、Paladin |
| 返回最老可用事件开始 | 跳过丢失的，可能漏掉中间变更 | 我们简化版 |

生产环境应该用策略 1——在 Day 6 的 SDK 中，收到 `ErrCompacted` 就触发 `fullPull()`。

### 面试灵魂拷问 #2

> **Q：环形缓冲区满了丢弃老事件，客户端会丢数据吗？**
>
> A：不丢**最终状态**，只丢**中间变更**。比如 db_host 从 v1→v2→v3→v4，如果 v1→v2 的事件被覆盖了，客户端全量重拉拿到的是 v4——最终状态是正确的。但如果业务需要审计每一次变更记录，就需要另外用 WAL 或数据库保存完整历史。

### 面试灵魂拷问 #3

> **Q：为什么用长轮询不用 WebSocket 或 gRPC 双向流？**
>
> A：长轮询对基础设施要求最低——穿透 HTTP 代理（Nginx、Envoy）无需额外配置。大型公司内网的 L7 代理对 HTTP 长连接支持比 WebSocket/gRPC 流更成熟。代价是每次超时后重新建连，连接数略高。如果重新做可以考虑 gRPC 双向流，但需要确保所有中间代理支持 HTTP/2。

---

## 7. 验证

### 运行测试

```bash
go test ./... -v
```

Store 包新增 7 个 Watch 测试：
- `TestWatchCacheAppendAndGet` — 基本追加和按 revision 查询
- `TestWatchCacheRingOverflow` — 容量溢出只保留最新 N 条
- `TestWatchCachePrefixFilter` — namespace 级别事件过滤
- `TestWatchCacheBlocking` — goroutine 阻塞，Append 后被唤醒
- `TestWatchCacheTimeout` — 超时返回空
- `TestWatchableStorePutEmitsEvent` — Put 自动产生事件（含 PrevEntry）
- `TestWatchableStoreDeleteEmitsEvent` — Delete 也产生事件

Server 包新增 4 个 Watch HTTP 测试：
- `TestWatchReturnsEventsImmediately` — 有事件立刻返回
- `TestWatchBlocksAndReturnsOnChange` — 先阻塞，PUT 后立刻返回
- `TestWatchTimeout` — 1 秒超时返回空事件列表
- `TestWatchPrefixFilter` — 只收到匹配 prefix 的事件

### curl 体验

终端 1 — 启动服务器：
```bash
go run ./cmd/paladin-core serve :8080
```

终端 2 — 发起 Watch（会阻塞 30 秒）：
```bash
curl "http://localhost:8080/api/v1/watch/public/prod/?revision=0&timeout=30"
```

终端 3 — 写入配置：
```bash
curl -X PUT http://localhost:8080/api/v1/config/public/prod/db_host -d '10.0.0.1'
```

终端 2 **立刻**收到事件并返回 ✅

---

## 8. 今天的收获

| 你学到了什么 | 面试中怎么说 |
|------------|-----------|
| 环形缓冲区 | "固定内存、O(1) 追加，通过 count % capacity 实现循环覆盖" |
| sync.Cond | "Wait 释放锁+阻塞、Broadcast 唤醒所有等待者，必须在 for 循环中调用" |
| 长轮询 vs WebSocket | "长轮询对 L7 代理最友好，trade-off 是连接数略高" |
| Event 含 PrevEntry | "让消费者做差异比较，SDK 可以触发精准的 OnChange 回调" |
| WatchableStore 组合 | "Go 的 embedding 让存储和事件无缝绑定，接口对上层透明" |

---

## 9. 明天预告：Day 4 — Raft 共识

Day 1-3 构建了功能完整的**单机**配置中心。但单机 = 单点故障。

明天引入 Raft 共识算法，把单机变成 3 节点集群。核心是实现 `raft.FSM` 接口——所有写操作先经 Raft log 复制到多数节点，然后再应用到 WatchableStore。你今天实现的 WatchableStore 接口不需要任何改动。
