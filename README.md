# PaladinCore — 7 天从零实现分布式配置中心

> 受 GeeCache 启发，参考 Bilibili Paladin 架构，~2000 行 Go 代码。

## 项目简介

PaladinCore 是一个 CP 模型的分布式配置中心，核心特性：

- **Raft 共识**（HashiCorp Raft）— 多节点强一致
- **BoltDB 持久化** — 事务级写入，崩溃安全
- **Watch 长轮询** — 环形缓冲区 + sync.Cond 实时推送
- **ForwardRPC** — Follower 透明代理写请求到 Leader
- **SDK 三级降级** — 全量拉取 → 长轮询增量 → 本地缓存兜底
- **Docker Compose** — 一键三节点集群

## 7 天教程目录

| Day | 主题 | 核心概念 | 教程 |
|-----|------|---------|------|
| 1 | [KV Store](doc/day1-kv-store.md) | BoltDB + revision 逻辑时钟 | [教程](doc/day1-kv-store.md) |
| 2 | [HTTP API](doc/day2-http-api.md) | RESTful + 多租户 namespace | [教程](doc/day2-http-api.md) |
| 3 | [Watch](doc/day3-watch.md) | 环形缓冲区 + 长轮询 | [教程](doc/day3-watch.md) |
| 4 | [Raft](doc/day4-raft.md) | FSM + log 复制 + Snapshot | [教程](doc/day4-raft.md) |
| 5 | [ForwardRPC](doc/day5-forward-rpc.md) | Leader 代理 + 一致性读 | [教程](doc/day5-forward-rpc.md) |
| 6 | [SDK](doc/day6-sdk-client.md) | 客户端生命周期 + 缓存降级 | [教程](doc/day6-sdk-client.md) |
| 7 | [集群部署](doc/day7-cluster-deploy.md) | Docker Compose + 故障自愈 | [教程](doc/day7-cluster-deploy.md) |

## 快速开始

### 单机模式
```bash
go run ./cmd/paladin-core serve :8080
```

### 三节点集群
```bash
docker compose up -d
```

### SDK 使用
```go
c, _ := sdk.New(sdk.Config{
    Addrs:     []string{"localhost:8080"},
    Tenant:    "public",
    Namespace: "prod",
    CacheDir:  "/tmp/paladin-cache",
})
val, _ := c.Get("public/prod/db_host")
c.OnChange("", func(key string, old, new []byte) {
    log.Printf("%s: %s → %s", key, old, new)
})
```

## 项目结构

```
paladin-core/
├── cmd/paladin-core/main.go     入口（standalone / cluster / CLI）
├── store/
│   ├── store.go                 Store 接口 + Entry 定义
│   ├── bolt.go                  BoltDB 实现
│   ├── watch.go                 WatchCache 环形缓冲区
│   └── watchable.go             WatchableStore（存储+事件桥梁）
├── server/
│   ├── server.go                HTTP API（CRUD + 路由）
│   ├── watch.go                 Watch 长轮询端点
│   └── raft_server.go           Raft 感知的 HTTP 服务（ForwardRPC）
├── raft/
│   └── node.go                  Raft 节点 + FSM + Snapshot
├── sdk/
│   └── client.go                Go SDK（全量拉取+Watch+缓存降级）
├── doc/                         7 篇教程文档
├── Dockerfile
└── docker-compose.yml           3 节点集群
```

## 与 Paladin 原版的对比

| 维度 | Paladin (~23,000 行) | PaladinCore (~2,000 行) |
|------|---------------------|------------------------|
| API 版本管理 | K8S 多版本 + 代码生成 | 单版本 v1 |
| 存储层 | 多层装饰器 | BoltDB + WatchCache |
| RPC | 自定义 GoRPC | HTTP 转发 |
| ACL | 完整访问控制 | JWT token |
| Message 模块 | 运维公告 | 不实现 |
