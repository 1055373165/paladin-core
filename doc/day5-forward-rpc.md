# Day 5：ForwardRPC + 一致性读 — 客户端无感知集群拓扑

> 今天的目标：Follower 自动转发写请求给 Leader，加入一致性读选项。
> 代码量：~250 行 | 新增文件：`server/raft_server.go`

---

## 1. 问题：客户端必须知道谁是 Leader

Day 4 的 Raft 实现有个严重的可用性问题：

```go
func (n *Node) Apply(op Op, timeout time.Duration) (*OpResult, error) {
    if n.raft.State() != raft.Leader {
        return nil, ErrNotLeader  // 非 Leader 直接报错！
    }
    // ...
}
```

生产环境中，客户端通过负载均衡器（如 Nginx）随机连到某个节点。3 节点集群中有 2/3 概率连到 Follower，直接报错显然不行。

---

## 2. ForwardRPC：透明 Leader 代理

解决方案：Follower 收到写请求时，**自动转发给 Leader**，客户端完全无感知。

```
Client ──PUT──→ Follower(Node2)
                    │
                    │ "我不是 Leader，帮你转发"
                    │
                    ↓
                Leader(Node1) ──Raft Apply──→ 成功
                    │
                    │ 返回结果
                    ↓
                Follower(Node2)
                    │
                    │ 原样返回给 Client
                    ↓
Client ←──201 Created (完全不知道发生了转发)
```

Paladin 原版用 RPC（gorpc）转发。我们用 HTTP 代理——更简单，效果相同：

```go
func (rs *RaftServer) forwardToLeader(w http.ResponseWriter, r *http.Request, body []byte) {
    leaderAddr := rs.node.LeaderAddr()  // 从本地 Raft 状态获取
    if leaderAddr == "" {
        // Leader 选举中（~1-3 秒窗口），返回 503
        httpError(w, 503, "no leader available, try again later")
        return
    }

    // 构造转发请求
    url := fmt.Sprintf("http://%s%s", leaderAddr, r.URL.String())
    fwdReq, _ := http.NewRequest(r.Method, url, bytes.NewReader(body))

    // 标记"我是转发的"，防止循环转发
    fwdReq.Header.Set("X-Forwarded-By", myNodeID)

    // 执行转发，把 Leader 的响应原样返回给客户端
    resp, err := http.DefaultClient.Do(fwdReq)
    if err != nil {
        httpError(w, 502, "forward failed: %v", err)
        return
    }
    // 复制响应头+状态码+body → 客户端
    copyResponse(w, resp)
}
```

### 面试灵魂拷问 #1

> **Q：ForwardRPC 的 Leader 地址从哪来？会不会过时？**
>
> A：从本地 Raft 状态机获取（`raft.LeaderWithID()`）。Raft 通过 heartbeat 保持 Leader 信息的时效性。如果 Leader 刚切换，本地可能短暂持有旧 Leader 地址——转发到旧 Leader 会得到 `ErrNotLeader`，此时返回 502 让客户端重试。生产实现应在 Follower 侧做 1-2 次重试（指数退避 100ms → 200ms），覆盖选举窗口。

---

## 3. 路由分发：读走本地，写走 Raft

```go
func (rs *RaftServer) handleRaftConfig(w, r) {
    switch r.Method {
    case GET:
        // 读操作 → 直接读本地 BoltDB（允许脏读）
        rs.handleGet(w, r, tenant, namespace, name)

    case PUT:
        if !rs.node.IsLeader() {
            rs.forwardToLeader(w, r, body)  // Follower → 转发
        } else {
            rs.node.Apply(Op{...})           // Leader → Raft Apply
        }

    case DELETE:
        // 同 PUT 的逻辑
    }
}
```

读和写走不同的路径——这是配置中心的典型模式（写少读多）。

---

## 4. 一致性读：两种策略

Follower 的数据可能落后 Leader 几个 log entry。这引出了分布式系统的经典问题：读的一致性。

| 模式 | 实现 | 一致性 | 延迟 | 使用场景 |
|------|------|--------|------|---------|
| **脏读**（默认） | 直接读本地 BoltDB | 可能落后几个 entry（~毫秒级） | 0 网络开销 | 大多数配置读取 |
| **强一致读** | 先 `raft.VerifyLeader()` 确认 Leader 身份再读 | 线性一致 | +1 RTT（quorum 心跳） | read-after-write |

`VerifyLeader()` 的工作原理：
1. Leader 向半数以上节点发心跳
2. 收到 quorum ACK → 确认自己仍是 Leader
3. 然后读本地数据——此时保证数据是最新的

```go
// 强一致读（需要时显式开启）
if r.URL.Query().Get("consistent") == "true" {
    if err := rs.raft.VerifyLeader().Error(); err != nil {
        httpError(w, 503, "not verified leader")
        return
    }
}
// 然后读本地数据
```

### 面试灵魂拷问 #2

> **Q：为什么不默认每次读都做 VerifyLeader？**
>
> A：开销问题。VerifyLeader 需要 quorum 心跳（内网 ~0.5ms RTT），如果几千个 Pod 同时启动并读配置，Leader 需要为每个读请求做一次 quorum 确认——这会给集群造成大量心跳压力。配置中心的读可以容忍毫秒级的不一致（配置不是每秒都在变），所以默认脏读，只在 read-after-write 场景显式要求强一致。

### 面试灵魂拷问 #3

> **Q：etcd 的线性一致读用的是什么方案？**
>
> A：etcd 早期用 VerifyLeader（和我们一样），后来改为 **ReadIndex**：Leader 记录当前 commitIndex，等 apply 到那个 index 后再返回。这比 VerifyLeader 更轻量——只需确认 commitIndex 已被 apply，不需要额外心跳。但实现更复杂。

---

## 5. Leader 不可用时的退避

Leader 切换（选举）通常需要 1-3 秒。这期间 `LeaderAddr()` 返回空。

```
Timeline:
  t=0s    Leader(Node1) 崩溃
  t=0s    Follower 检测到心跳超时
  t=1-3s  选举进行中（LeaderAddr = ""）
  t=3s    新 Leader(Node2) 当选
```

我们的做法：返回 `503 Service Unavailable`，SDK 侧做指数退避重试（Day 6）。

生产做法（Paladin 的 `ForwardRPC`）：在 Follower 侧对同一请求做内部重试 100ms → 200ms → 400ms，总计覆盖 ~1 秒选举窗口，大多数情况下客户端感知不到切换。

---

## 6. Admin 接口

Day 5 同时实现了集群管理接口：

```
POST /admin/join?id=node2&addr=127.0.0.1:9002
  → rs.node.Join("node2", "127.0.0.1:9002")
  → raft.AddVoter()

POST /admin/leave?id=node2
  → raft.RemoveServer()

GET /admin/stats
  → {"state":"Leader", "term":"3", "commit_index":"42", "store_revision":"15"}
```

这些就是你在 Paladin 实习时做的 Admin 模块的简化版。

---

## 7. 验证

编译通过，全量 29 测试通过。

手动验证（3 节点）：
```bash
# 写到 Follower → 自动转发到 Leader → 成功
curl -X PUT http://localhost:8081/api/v1/config/public/prod/key -d 'value'
# 返回 201 Created

# 查看转发日志
# [FORWARD] PUT /api/v1/config/public/prod/key → leader 127.0.0.1:8080 (status 201)
```

---

## 8. 今天的收获

| 你学到了什么 | 面试中怎么说 |
|------------|-----------|
| ForwardRPC | "Follower 透明代理写请求到 Leader，客户端无感知集群拓扑" |
| 脏读 vs 强一致读 | "默认脏读（零网络开销），需要时 VerifyLeader（+1 RTT quorum 心跳）" |
| 选举窗口退避 | "1-3 秒选举窗口内返回 503 或内部指数退避重试" |
| ReadIndex 优化 | "etcd 用 ReadIndex 替代 VerifyLeader，更轻量但更复杂" |

---

## 9. 明天预告：Day 6 — SDK 客户端

服务端已经完整了。明天实现客户端 SDK——启动全量拉取 → 后台长轮询 → 本地缓存降级。这正是你在 B 站设计的 SDK V2 的核心架构。
