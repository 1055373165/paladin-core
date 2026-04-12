# Day 6：SDK 客户端 — 长轮询 + 本地缓存降级

> 今天的目标：实现 Go SDK，三阶段生命周期 + SHA-256 本地缓存。
> 代码量：~300 行 | 新增：`sdk/client.go`、`sdk/client_test.go`

---

## 1. SDK 的三阶段生命周期

这正是你在 B 站为 Paladin SDK V2 设计的架构：

```
    Phase 1: Startup
    ┌─────────────────────────┐
    │ 尝试全量拉取 ──失败──→ 本地缓存 │
    └────────────┬────────────┘
                 ↓
    Phase 2: Runtime (后台 goroutine)
    ┌─────────────────────────┐
    │ while true:             │
    │   long poll(revision=N) │
    │   if events → 更新内存   │
    │            → 写本地缓存  │
    │   if timeout → 继续      │
    │   if error → 退避重试    │
    └────────────┬────────────┘
                 ↓
    Phase 3: Shutdown
    ┌─────────────────────────┐
    │ cancel context → 停止轮询 │
    └─────────────────────────┘
```

## 2. 全量拉取

```go
func (c *Client) fullPull() error {
    resp := GET("/api/v1/config/{tenant}/{namespace}/")
    for _, item := range resp.Configs {
        c.configs[item.Key] = item.Value
    }
    c.revision = resp.Revision
    c.saveToCache()  // 立刻持久化，为后续降级做准备
}
```

失败时的降级路径：
```
fullPull() 失败 → loadFromCache() → 验证 SHA-256 → 加载到内存
                                  → checksum 不匹配 → 拒绝加载（文件可能损坏）
                                  → 文件不存在 → 空配置启动
```

## 3. 长轮询 Watch 循环

```go
func (c *Client) watchLoop() {
    for {
        resp := GET("/api/v1/watch/{tenant}/{namespace}/?revision=N&timeout=30")
        if events → applyEvents() + saveToCache()
        if error → sleep(retryBackoff) + retry
    }
}
```

关键细节：
- `http.Client.Timeout` 设为 `PollTimeout + 5s` — 比服务端超时多留 5 秒，避免客户端先超时
- 用 `context.WithCancel` 控制生命周期，Close() 时 cancel → watchLoop 退出

## 4. OnChange 回调

```go
c.OnChange("public/prod/db_host", func(key string, old, new []byte) {
    log.Printf("db_host changed: %s → %s", old, new)
    reconnectDB(string(new))
})
```

支持 key="" 的通配符监听（监听所有变更）。回调在 applyEvents 的锁内同步执行——生产环境应该用 channel 异步通知，避免回调阻塞 watch 循环。

## 5. 本地缓存 + Checksum

缓存文件格式：
```json
{
  "checksum": "a1b2c3...sha256...",
  "revision": 42,
  "configs": {
    "public/prod/db_host": "10.0.0.1",
    "public/prod/db_port": "3306"
  }
}
```

SHA-256 作用：检测文件损坏（磁盘错误、进程写一半被 kill）。加载时先验证 checksum，不匹配则拒绝使用。

### 面试灵魂拷问

> **Q：本地缓存降级意味着可能用过期配置启动，安全吗？**
>
> A：取决于配置类型。数据库地址通常很少变，用缓存没问题。但限流阈值如果刚被紧急调低，用缓存可能导致过载。生产 SDK 应提供 `RequireFresh(key)` 选项——关键配置拿不到最新值就 panic 不启动。

## 6. 验证

5/5 测试通过：
- `TestSDKFullPullAndGet` — 连接服务器全量拉取
- `TestSDKWatchUpdates` — 配置变更通过 Watch 推送到 SDK
- `TestSDKCacheFallback` — 服务器宕机后从缓存恢复
- `TestSDKCacheChecksumValidation` — 损坏的缓存被拒绝
- `TestSDKServerDown` — 完全不可用时不 panic，空配置启动
