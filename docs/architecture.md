# 系统架构设计

## 控制面与数据面

Master FastAPI 是控制面，负责在线节点、文件状态、任务状态及 Shuffle。Client 与 Worker 直接传输文件数据；为防止客户端错误确认，Master 在标记副本 stored 前回读并校验一次副本，因此教学版上传会多传输一份验证数据。Dashboard 的所有集群操作经统一 API Client 执行，不自行调度任务或制造状态。

Worker FastAPI 维护不可变 Chunk 文件，计算通过 `ProcessPoolExecutor` 的 spawn 模式进入子进程。网络服务事件循环保持可发送心跳；CPU 算法不在 HTTP 线程中运行。每节点一个独立根目录和端口。

## 状态与并发

Master 以单进程 Uvicorn、一个 asyncio 事件循环运行。状态修改不跨线程；进程管理在线程里操作独立、OS 文件锁保护的 PID 注册表。调度槽位的检查与占用在 `asyncio.Lock` 中原子完成。不要以 `uvicorn --workers N` 启动 Master，否则各进程状态会分裂。

文件 allocate 创建有序 Chunk 与 distinct Worker 副本。副本 stored 表示曾验证成功，节点 online 表示最近收到心跳；两者分开。文件 complete 只检查各块至少一个成功副本，健康度实时计算在线成功副本数量：0 Unavailable、1 Degraded、>=2 Healthy。

状态使用 `write -> flush -> fsync -> os.replace` 原子保存 JSON。启动时读取文件、Job 和 Benchmark；Master 重启时未完成 Job/Benchmark 明确转 failed。未进行分布式元数据共识或运行中任务的日志恢复。

## 调度与数据本地性

WordCount 一块一任务。提交时固定候选 Worker 集合（最多请求数量），任务分配优先未失败过的节点、持有 stored 副本的节点、较低在途负载；只有有空槽的在线节点才参与选择。若候选全忙则排队；若全部离线持续 20 秒则失败。因容错减少 Worker 数时不扩充到用户指定候选集合外。

Map 输出首先原子保存于 Worker `results/<attempt_id>.json`，Master 获取并保留该尝试结果，然后才确认逻辑 Task 完成。已经获取的结果在 Worker 后续离线时仍可用于 Shuffle，避免因中间结果遗失而重复累加。代价是 Master 内存占用。

Shuffle 使用稳定的 SHA-256 前 8 字节对 Reducer 数取模；相同 Key 唯一分区。WordCount Map 含本地 Combiner，Shuffle 值列表为各 Map 的计数。Reduce 在 Worker 进程聚合，分区结果以键并集合并。Monte Carlo Reduce 汇总总样本和圆内点整数计数。

## Attempts 与失败

每个逻辑任务最多首次执行 + 3 次重试；每次使用全新 attempt_id。Worker 对同 attempt_id 相同请求幂等，对不同 payload 返回 409。Master 轮询唯一当前 attempt，不开放“任意完成回调”接口，因此旧尝试迟到完成或重启前任务结果不能覆盖当前结果。远端旧计算可能继续到超时结束，但不计入聚合；不提供强制取消单个计算进程的 API。

整个 stage 等待全部任务 settle 后才进入下阶段；任何任务耗尽重试会使 Job failed，不提交部分最终结果。计算时可在 HTTP 连接失败前后感知节点故障；状态页仍按心跳期限决定 Offline。

## 本机运行与边界

所有默认监听地址为环回地址。进程管理记录完整启动参数与创建时间，停止前再次校验身份，子进程只从经校验父进程树枚举。无 API 接收 shell 命令、Python 源码或任意脚本路径。对象名只允许字母、数字、下划线和短横线，并验证解析路径位于配置数据目录内。

本项目是 GFS/MapReduce 核心思想的缩小实现，不应把同机多进程实验结论外推为多台物理机的扩展性。

## 原始论文

- Sanjay Ghemawat, Howard Gobioff, Shun-Tak Leung. [The Google File System, SOSP 2003](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf)。本原型借鉴 Master/Chunk、多副本与故障检测的思想，不复刻其完整写入协议。
- Jeffrey Dean, Sanjay Ghemawat. [MapReduce: Simplified Data Processing on Large Clusters, OSDI 2004](https://static.usenix.org/event/osdi04/tech/full_papers/dean/dean_html/index.html)。本原型实现 Map、按 Key 分组、Reduce、任务重试和数据本地性；集中式内存 Shuffle 是为教学简化作出的实现选择。
