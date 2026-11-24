# 开发模块与扩展说明

## 核心实现入口

| 模块 | 责任 |
|---|---|
| common/models.py | WorkerInfo、FileCreate、JobCreate、TaskRequest、BenchmarkCreate 及响应模型 |
| config/settings.py | 数据根目录、Master 地址、块大小、心跳、离线、并发、重试、任务超时 |
| minigfs/master.py | FastAPI 路由、状态校验、监控生命周期 |
| minigfs/metadata.py | JSON 持久化、在线状态、健康度及事件队列 |
| minigfs/client.py | 两遍流式扫描上传、逐块副本写入、下载回退与原子替换 |
| minimr/scheduler.py | 任务生成、槽位、Locality、Attempts、Shuffle、阶段推进 |
| minimr/algorithms.py | 单机与分布式共同使用的算法定义 |
| worker/server.py | HTTP 服务、心跳、异步结果管理、进程池生命周期 |
| worker/executor.py | CPU 子进程入口、Chunk 获取与校验、算法分派 |
| scripts/process_manager.py | 跨进程文件锁、进程身份校验、局部进程管理 |
| benchmarks/runner.py | 后台实验、固定工作量基准、精确结果比较、导出 |
| dashboard/api_client.py | UI 统一访问后端，继承文件 Client |

## 可配置环境变量

| 变量 | 默认值 | 用途 |
|---|---|---|
| MINI_DATA | 项目 data 目录 | 每次实验可用独立根目录 |
| MINI_MASTER | http://127.0.0.1:8000 | Client/Worker/Dashboard 的后端地址 |
| MINI_HEARTBEAT | 3 | 心跳秒数 |
| MINI_OFFLINE_AFTER | 10 | 离线阈值秒数 |
| MINI_SLOTS | 1 | Worker 默认进程池槽数 |
| MINI_TASK_TIMEOUT | 600 | 每次任务尝试超时秒数 |

启动器命令行支持 Worker 数、端口及槽数，并将地址和数据目录传给子进程。修改环境变量应在启动进程之前完成。

## 对接其他版本后端

优先保持 `dashboard/api_client.py` 与页面之间返回对象字段稳定，将地址、路由或字段适配集中于 API 层。文件页上传/下载目前复用 `minigfs.client.Client` 的副本协议；若其他后端使用服务端代理上传，应在 API 层转换，不能在页面内重新实现调度。

前端重点依赖：Worker online/cpu/memory/used，File chunks/replicas/health，Job status/map_tasks/reduce_tasks/phases/result/intermediates，Task attempts/started_at/finished_at/assigned_worker。状态颜色和图表在 components.py，共用 Plotly 主题。

## 测试层次

Pytest 使用真实本地临时存储、TestClient 与真实关闭端口进行失败测试；不靠 Mock 成功结果证明功能。Integration 使用真实 Uvicorn 服务、多进程、HTTP、磁盘文件与进程终止。AppTest 执行 Streamlit 页面代码连接真实后端；实际浏览器用于确认视觉布局，截图不能替代后端结果验证。

## 已实现与尚未实现

已实现 P0，以及 JSON 元数据持久化、两类三次重复基准、Gantt、资源历史、受控故障面板、历史与导出。未实现 P2 的自动副本修复、压缩、高级调度、并发文件上传优化。无 Master 高可用，未提供进行中 Job 跨 Master 重启恢复。

后续改进方向：用 SQLite/独立结果文件替代整份 JSON 快照；Shuffle 分页/流式传输；优化远程边界读取；实现精确应用字节与网络抓包指标；增加身份认证；对失效副本重建；将资源采样口径细化到各计算子进程。
