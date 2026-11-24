# 实验设计与评价

## 实验矩阵

WordCount：固定文本及 SHA-256，默认 Chunk 4 MiB、固定 Reducer 数；建议 1/10/50/100 MiB。Monte Carlo：固定 seed、固定总样本和分片数；建议 1M/5M/10M/50M。两者均比较串行、1/2/3 Worker（启动更多 Worker 可扩展）。默认低负载实验，不在启动时自动做大型实验。

每组至少 3 次，保留每次原始值，计算 mean、median、sample standard deviation、min、max。不排除首轮冷启动，不删除慢样本。分片数量保持固定，不因 Worker 数变化改变每个分片的随机流；每个结果都与串行整数计数/完整字典精确比较。

## 时间定义

- `seconds`：分布式 Job 从开始调度到最终结果提交的墙钟时间，不含原始文件上传；串行原始行此字段为进程池调用端到端时间（包含首次 spawn）。
- `compute_seconds`：串行行是算法及本地输入读取时间；分布式行是成功 Task 的 Worker 执行时间之和（含 Map 必需的本地/远程 Chunk 读取），不是并行墙钟时间。
- `map`、`reduce`：各阶段墙钟时间，包括排队、网络、轮询、必要的重试。
- `shuffle`：Master 分组及分区的墙钟时间；传给 Reduce Worker 的网络时间在 Reduce 阶段。
- `coordination`：阶段计时以外的协调时间；并非所有调度开销，禁止把它当作网络或排队总开销。
- `transfer_bytes`：Task 请求 JSON、成功结果响应和远程 Chunk 读取的应用载荷字节。不是抓包统计，不含 HTTP/TCP 头、心跳、轮询、失败请求或原始文件上传。JSON 序列化空格可能使请求计数与 wire payload 略有差异。
- `retries`：各 Task `(attempt-1)` 之和。

主图 Speedup = 串行 `compute_seconds` 均值 / 分布式 `seconds` 均值。Efficiency = Speedup / Workers ×100%。这是纯算法与完整分布式系统的应用级比较，不能声称测得纯 CPU 并行效率。完整 CSV 同时保留两种计时，允许另外分析端到端基线。

## 公平性与限制

CPU 算法、总样本、输入、随机种子和分片始终固定。串行计算在单个 spawn 子进程中运行，与 Worker 使用同一算法。WordCount 串行按完整输入计数，分布式需要额外边界读取、Combiner 和 Shuffle，因此额外字节读取和通信是框架开销，不计为算法工作量不同。

实验启动前拒绝已有活动 Job/Benchmark。但运行期间用户仍可手动提交其他 Job、停止 Worker，外部程序也可能占用 CPU；严格测量时请关闭自动刷新、不要并发交互，并查看环境和事件记录。Worker 进程池可能在此前实验中已预热；首轮与后续轮不可被假定为相同冷启动状态。

CPU 指标为主机总使用率，各 Worker 同机采集不能相加。内存为进程树 RSS，不能直接等同于唯一物理内存。串行基准前后耗时均留存，资源时间线主要采样分布式阶段。实验资源采样上限 12000 条，达到上限保留最近部分。

本机实测小规模任务比串行慢属于真实结果：HTTP 客户端、进程池、轮询、分区和结果序列化可能压过计算收益。报告必须展示慢于串行的加速比，不调整数据制造线性曲线。

## 导出与复现

`data/benchmarks/<id>/results.json` 包含全部配置、硬件/系统/Python、原始行、资源采样和汇总；`results.csv` 是原始实验行。Dashboard 可直接导出同样的 JSON/CSV。Plotly 工具栏支持浏览器端 PNG 导出，不依赖 Kaleido。版本记录参见 `docs/environment.json`。

本次真实运行的精简归档位于 `docs/benchmark-monte-carlo.json` 和 `docs/benchmark-wordcount.json`；不代表其他硬件或更大样本的性能。
