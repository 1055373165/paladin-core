# Day 2：HTTP API + 多租户 — 让配置可被远程访问

> 今天的目标：在 Day 1 的 KV Store 上面加 HTTP API，引入 tenant/namespace/name 三级结构。
> 代码量：~250 行 | 新增文件：`server/server.go`、`server/server_test.go`，修改 `cmd/paladin-core/main.go`

---

## 1. 从本地到远程

Day 1 的 Store 只能通过 CLI 访问——你必须坐在服务器面前才能改配置。这在生产环境不现实。

今天我们加一层 HTTP API：

```
运维人员 ──PUT──→ [ HTTP Server ] ──→ [ BoltDB Store ]
                                          ↑
Service A ──GET──→ [ HTTP Server ] ──────┘
```

这是所有配置中心的基本形态：一个 HTTP（或 RPC）服务端 + 一个持久化存储。

---

## 2. 为什么用 tenant/namespace/name 三级结构？

一个扁平的 key（如 `db_host`）在小项目里没问题，但在 B 站这种有上千个微服务的环境下，扁平 key 会立刻遇到命名冲突和权限隔离问题。

Paladin 的解决方案是三级结构：

```
tenant / namespace / name
  │        │         │
  │        │         └── 具体配置项（如 db_host, redis_port）
  │        └── 服务标识（如 prod-live, staging-comment）
  └── 租户/组织（如 public, payment）
```

**映射到实际场景：**

| tenant | namespace | name | value |
|--------|-----------|------|-------|
| public | prod-live | db_host | 10.0.0.1 |
| public | prod-live | db_port | 3306 |
| public | staging-live | db_host | 10.0.1.1 |
| payment | prod-pay | stripe_key | sk_xxx |

这直接映射到 K8S 的资源路径：`/api/v1/{resource}/{namespace}/{name}`。面试时你可以说"参考了 K8S API 设计"。

### 面试灵魂拷问

> **Q：为什么用 URL 路径而不是 query parameter 来表达 tenant/namespace？**
>
> A：路径是资源的标识符，query parameter 是过滤条件。`/config/public/prod/db_host` 语义清晰：这是一个固定的资源地址，可以被缓存、被书签、被 RPC 代理。如果用 `?tenant=public&namespace=prod&name=db_host`，每次解析都需要拼字符串，而且 HTTP 缓存无法区分不同参数组合的响应。

---

## 3. HTTP 状态码的语义

一个容易被忽略但面试会问的细节：PUT 请求的响应状态码。

```go
status := http.StatusOK          // 200: 更新已有配置
if result.PrevEntry == nil {
    status = http.StatusCreated   // 201: 创建新配置
}
```

为什么要区分？因为客户端需要知道"这是新建还是覆盖"。比如一个部署脚本执行 PUT，如果返回 201 说明这个配置是首次创建（可能需要通知运维审查），200 说明覆盖了已有值。

同时，响应头里带了 `X-Paladin-Revision`：

```
HTTP/1.1 201 Created
X-Paladin-Revision: 5
Content-Type: application/json
```

Day 3 的 Watch 长轮询就靠这个 Revision 来确定"从哪里开始监听变更"。

---

## 4. 核心实现

### 4.1 路由设计

```go
func (s *Server) routes() {
    s.mux.HandleFunc("/api/v1/config/", s.handleConfig)  // CRUD
    s.mux.HandleFunc("/api/v1/rev", s.handleRev)          // 查看 revision
    s.mux.HandleFunc("/healthz", ...)                     // 健康检查
}
```

所有 config 相关操作走 `/api/v1/config/` 前缀，内部按 HTTP Method 分发：

| Method | Path | 语义 |
|--------|------|------|
| GET | `/api/v1/config/public/prod/db_host` | 获取单个配置 |
| GET | `/api/v1/config/public/prod/` | 列出 prod namespace 下所有配置 |
| GET | `/api/v1/config/public/` | 列出 public tenant 下所有配置 |
| PUT | `/api/v1/config/public/prod/db_host` | 创建/更新配置 |
| DELETE | `/api/v1/config/public/prod/db_host` | 删除配置 |

### 4.2 URL 解析

```go
func configKey(path string) (tenant, namespace, name string, err error) {
    trimmed := strings.TrimPrefix(path, "/api/v1/config/")
    parts := strings.SplitN(trimmed, "/", 3)
    // parts[0] = tenant, parts[1] = namespace, parts[2] = name
}
```

路径段数决定操作语义：
- 1 段（tenant）→ 列出该 tenant 下所有配置
- 2 段（tenant/namespace）→ 列出该 namespace 下所有配置
- 3 段（tenant/namespace/name）→ 操作具体的一个配置项

### 4.3 内部 store key 的构造

外部 URL 和内部 store key 的映射：

```
URL:  /api/v1/config/public/prod/db_host
Key:  public/prod/db_host
```

列表操作用 prefix scan：

```go
func listPrefix(tenant, namespace string) string {
    if namespace == "" {
        return tenant + "/"        // "public/" → 列出所有
    }
    return tenant + "/" + namespace + "/"  // "public/prod/" → 只列 prod
}
```

这利用了 Day 1 中 BoltDB cursor 的 `Seek` + prefix 匹配——BoltDB 的 B+ tree 按 key 排序存储，所以 prefix scan 是 O(log n + k) 的。

---

## 5. 响应格式

统一的 JSON 响应格式：

```json
{
  "revision": 5,
  "count": 2,
  "configs": [
    {
      "key": "public/prod/db_host",
      "value": "10.0.0.1",
      "revision": 3,
      "create_revision": 1,
      "mod_revision": 3,
      "version": 2
    },
    {
      "key": "public/prod/db_port",
      "value": "3306",
      "revision": 2,
      "create_revision": 2,
      "mod_revision": 2,
      "version": 1
    }
  ]
}
```

顶层的 `revision` 是当前 store 的全局 revision，每个 config 里的 `revision` 是该 key 当前值对应的 revision。客户端可以用顶层 `revision` 来做后续 Watch。

---

## 6. 验证

### 运行测试

```bash
go test ./... -v
```

预期输出：13/13 PASS（7 store + 6 server）

### curl 体验

启动服务器：
```bash
go run ./cmd/paladin-core serve :8080
```

另一个终端：
```bash
# 创建配置
curl -X PUT http://localhost:8080/api/v1/config/public/prod/db_host \
  -d '10.0.0.1'
# {"revision":1,"configs":[{"key":"public/prod/db_host","value":"10.0.0.1",...}]}

# 查询配置
curl http://localhost:8080/api/v1/config/public/prod/db_host

# 列出 namespace 下所有配置
curl http://localhost:8080/api/v1/config/public/prod/

# 查看当前 revision
curl http://localhost:8080/api/v1/rev
```

---

## 7. 今天的收获

| 你学到了什么 | 面试中怎么说 |
|------------|-----------|
| tenant/namespace/name 三级结构 | "参考 K8S API 设计，结构化 key 比扁平 key 可管理性更强" |
| 201 vs 200 语义区分 | "PUT 的幂等性和状态码语义，区分创建和覆盖让客户端能做不同处理" |
| prefix scan 的效率 | "BoltDB 的 B+ tree 按 key 排序，prefix scan 是 O(log n + k)" |
| URL 路径 vs query parameter | "路径是资源标识符，可以被 HTTP 缓存；query parameter 是过滤条件" |

---

## 8. 明天预告：Day 3 — Watch 长轮询

今天的 HTTP API 是请求-响应模型（拉模式）：客户端必须主动来问"配置变了吗？"

明天我们实现 Watch 机制——客户端发一个请求然后阻塞等待，服务端有变更时立刻推送。这就是"从拉变推"，也是 SDK V2 长轮询的实现基础。核心数据结构是环形缓冲区（ring buffer）+ `sync.Cond`。
