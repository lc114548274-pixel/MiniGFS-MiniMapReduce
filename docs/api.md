# REST API

默认 Master `http://127.0.0.1:8000`；完整请求/文件/任务/Job 响应模型见 `/docs` 和 `/openapi.json`。所有 JSON 使用 UTF-8，时间为 UTC ISO 8601。默认 HTTPX 不读取代理环境变量，避免 localhost 流量被转至代理。

| Method | Master path | 语义 |
|---|---|---|
| GET | /health | 健康检查 |
| POST | /workers/register | 注册 Worker |
| POST | /workers/heartbeat | 注册或刷新心跳 |
| GET | /workers | 当前 Worker 信息与 online 标记 |
| POST | /files/allocate | 分配文件、Chunk ID 与副本（201） |
| POST | /files/{id}/replicas | 验证并更新一个副本状态 |
| POST | /files/{id}/complete | 完成上传或标记失败 |
| GET | /files | 文件列表，含当前副本健康度 |
| GET | /files/{id} | 文件详情及副本 URL |
| GET | /files/{id}/preview | 从真实副本读取原文前 2 KiB，校验后返回 |
| POST | /files/{id}/download-report | 记录 Client 校验报告（客户端声明） |
| POST | /jobs | 提交 Job，202 返回初始状态 |
| GET | /jobs | Job 摘要，不含大型结果 |
| GET | /jobs/{id} | Job、所有 Task、尝试、中间及最终结果 |
| GET | /jobs/{id}/tasks | 所有 Map/Reduce Task |
| GET | /jobs/{id}/result | 已完成结果，未完成返回 409 |
| GET | /metrics | 聚合指标和有界监控历史 |
| GET | /events | 最近 2000 条事件，可筛 level/worker_id/job_id/event |
| POST | /benchmarks | 后台性能实验，至少三次重复 |
| GET | /benchmarks | 历史实验 |
| GET | /benchmarks/{id} | 配置、环境、原始行及统计摘要 |
| GET | /processes | 项目启动器记录及身份核验后的 alive 状态 |
| POST | /processes/{worker-id}/{action} | action 仅 start/stop/restart，限已登记 Worker |

Worker 默认端口 9001/9002/9003：

| Method | Worker path | 语义 |
|---|---|---|
| GET | /health | 身份与 instance_id |
| PUT | /chunks/{id} | 原始字节，`X-Checksum` 必须为 SHA-256；块最大 16 MiB |
| GET | /chunks/{id} | 原始字节；Client/Mapper 验证 checksum |
| POST | /tasks/map | TaskRequest，202 接受或幂等返回 |
| POST | /tasks/reduce | TaskRequest，202 接受或幂等返回 |
| GET | /tasks/{attempt_id} | 某次尝试状态 |
| GET | /results/{attempt_id} | 已落盘结果及实际计算耗时 |

## 提交示例

```json
{"job_type":"monte_carlo","samples":1000000,"partitions":6,"seed":42,"worker_count":3,"max_concurrency":1}
```

```json
{"job_type":"wordcount","input_file_id":"上传返回的ID","reducers":2,"worker_count":3}
```

```json
{"job_type":"monte_carlo","samples":1000000,"partitions":6,"worker_counts":[1,2,3],"repeats":3,"seed":42}
```

文件 allocate 请求包含 filename、file_size、文件 checksum、按顺序排列的 `{size, checksum}` chunks。空文件使用空列表。响应为有序块及副本列表。Client 对每个副本 PUT 后发送 `{chunk_id, worker_id, status:"stored"或"failed"}`，再调用 complete。

错误：404 未找到；409 状态冲突/幂等键冲突/校验失败；413 Chunk 过大；422 参数不合法；429 Worker 槽已满；503 无 Worker。请求超时：普通控制请求 30 秒、Master 到 Worker 10 秒、心跳 3 秒、Task 总时限默认 600 秒。任务请求幂等；重复提交 `/jobs` 被视为新 Job，不提供跨客户端提交的去重键。

本地教学服务无认证，不能直接对公网开放。下载报告是客户端校验声明，不是远端可信审计证据。
