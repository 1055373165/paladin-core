# Day 7：集群部署 + 全链路验证

> 最后一天：Docker Compose 三节点部署，集群管理接口，全链路串联。
> 新增：`Dockerfile`、`docker-compose.yml`，修改 `cmd/paladin-core/main.go`

---

## 1. 三节点部署架构

```
                    ┌──────────────┐
         :8080      │    Node1     │  bootstrap=true (初始 Leader)
    ────────────→   │  raft:9001   │
                    └──────┬───────┘
                           │ Raft AppendEntries
              ┌────────────┴────────────┐
              ↓                         ↓
    ┌──────────────┐          ┌──────────────┐
    │    Node2     │  :8081   │    Node3     │  :8082
    │  raft:9001   │          │  raft:9001   │
    └──────────────┘          └──────────────┘
```

`docker compose up -d` 启动：
1. Node1 先启动并 bootstrap（成为 Leader）
2. Node2/3 等 Node1 健康后通过 `--join node1:8080` 加入集群
3. 三个节点通过 Raft 保持数据一致

## 2. cluster 命令

```bash
# 启动 Leader（本地调试）
paladin-core cluster --id node1 --raft 127.0.0.1:9001 --http :8080 --bootstrap

# 加入集群
paladin-core cluster --id node2 --raft 127.0.0.1:9002 --http :8081 --join localhost:8080
paladin-core cluster --id node3 --raft 127.0.0.1:9003 --http :8082 --join localhost:8080
```

`--join` 的工作方式：启动后向 Leader 的 `/admin/join` 发 POST 请求，Leader 调 `raft.AddVoter()` 把新节点纳入集群。

## 3. Admin 接口

| 端点 | 方法 | 功能 |
|------|------|------|
| `POST /admin/join?id=N&addr=A` | 新节点加入 |
| `POST /admin/leave?id=N` | 节点退出 |
| `GET /admin/stats` | Raft 统计（state, term, commit_index, applied_index, revision） |

`/admin/stats` 输出示例：
```json
{
  "state": "Leader",
  "term": "3",
  "commit_index": "42",
  "applied_index": "42",
  "store_revision": "15",
  "is_leader": "true"
}
```

## 4. 全链路验证

### 测试 1：写入 → 集群复制

```bash
# 写到 Node1 (Leader)
curl -X PUT http://localhost:8080/api/v1/config/public/prod/db_host -d '10.0.0.1'

# 从 Node2 (Follower) 读 — 数据已复制
curl http://localhost:8081/api/v1/config/public/prod/db_host
```

### 测试 2：Follower 写入 → 自动转发

```bash
# 写到 Node2 (Follower) — ForwardRPC 自动转发到 Leader
curl -X PUT http://localhost:8081/api/v1/config/public/prod/new_key -d 'value'
# 返回 201 Created（客户端无感知转发）
```

### 测试 3：Kill Leader → 自动选举

```bash
docker compose stop node1

# 等 2-3 秒，Node2 或 Node3 成为新 Leader
curl http://localhost:8081/admin/stats | jq .state
# "Leader"

# 写入仍然可用
curl -X PUT http://localhost:8081/api/v1/config/public/prod/failover_test -d 'ok'
```

### 测试 4：SDK 端到端

```go
c, _ := sdk.New(sdk.Config{
    Addrs:     []string{"localhost:8080", "localhost:8081", "localhost:8082"},
    Tenant:    "public",
    Namespace: "prod",
    CacheDir:  "/tmp/paladin-cache",
})

c.OnChange("", func(key string, old, new []byte) {
    log.Printf("config changed: %s = %s", key, new)
})

// 在另一个终端 PUT 配置 → SDK 自动收到通知
```

## 5. Snapshot 与日志压缩

Raft 配置里 `SnapshotThreshold = 1024`，即每 1024 条 log 后触发 Snapshot：
1. 调用 `FSM.Snapshot()` — 导出所有 Entry 为 JSON
2. 持久化到磁盘
3. 截断已 apply 的 log

新节点加入时如果 log 落后太多，Leader 直接发 Snapshot 安装。

### 面试灵魂拷问

> **Q：为什么需要 Snapshot 而不是直接重放所有 log？**
>
> A：Log 会被压缩（截断）。运行一段时间后，老 log 已被 Snapshot 替代并删除。新节点此时没有老 log 可重放，只能从 Snapshot 安装，再从 Snapshot 之后的 log 追赶。这和 Redis AOF 重写、MySQL binlog purge 是同一个道理。

---

## 6. 项目总代码统计

```
store/       store.go + bolt.go + watch.go + watchable.go     ~450 行
server/      server.go + watch.go + raft_server.go            ~430 行
raft/        node.go                                          ~280 行
sdk/         client.go                                        ~260 行
cmd/         main.go                                          ~160 行
tests        bolt_test + watch_test + server_test +           ~420 行
             watch_test + node_test + client_test
─────────────────────────────────────────────────
总计                                                          ~2000 行
```

## 7. 完成！你现在拥有了什么

你从零实现了一个完整的分布式配置中心，覆盖了面试中所有核心问题：

| Day | 你实现了什么 | 对应的面试知识点 |
|-----|------------|----------------|
| 1 | BoltDB KV + revision | 逻辑时钟 vs 物理时钟、事务原子性 |
| 2 | HTTP API + 多租户 | RESTful 设计、K8S 风格资源路径 |
| 3 | Watch 环形缓冲区 | sync.Cond、长轮询 vs WebSocket vs gRPC流 |
| 4 | Raft FSM | 共识算法、log 复制、Snapshot |
| 5 | ForwardRPC | Leader 透明代理、一致性读 trade-off |
| 6 | SDK 客户端 | 缓存降级、checksum 校验、优雅停止 |
| 7 | Docker 集群 | 节点生命周期、故障自愈 |
