# 实际验证记录

验证日期：2026-11-24，Windows，Python 3.12.7。所有以下结果均来自实际执行，不以设计或静态代码检查代替运行验证。

## 自动化测试

`python -m pytest -q --junitxml=docs/pytest-results.xml`：**36 passed，2 warnings，9.51 秒**。

覆盖多种 Chunk 边界（包括 1 字节块、跨多个块的长单词和 UTF-8）、顺序恢复、SHA-256、不可变写入、路径限制、固定种子和采样总数、Shuffle/Reduce、1/2/3 Worker 副本分配、无节点拒绝、心跳离线/返回、元数据恢复、副本状态、数据本地性、真实连接失败的最大重试限制、进程身份及非项目进程拒绝。

2 条第三方弃用警告分别来自本机已有 AnyIO Pytest 插件和 Starlette TestClient 的 HTTPX 迁移提示，不是测试失败。项目不使用 pytest-asyncio，已通过 pytest.ini 排除本机不兼容旧插件。

## 真实多进程集成

`python -m scripts.integration_test`：**13 个场景全部通过，90.93 秒**。

1. 三个真实 Worker 注册。
2. 超过 4 MiB 文本、多 Chunk、双副本。
3. 上传下载及全文件 SHA-256 一致。
4. 磁盘副本实际损坏后校验失败并切换副本。
5. 空文件及二进制文件上传下载。
6. 真正执行 Mapper 处理 7 字节块、跨块 UTF-8 和长英文单词。
7. 停止真实 Worker，检测离线并通过剩余副本下载。
8. WordCount 完整 Map/Shuffle/Reduce 与串行字典一致。
9. 1200 万次 Monte Carlo 运行中停止 Worker，发生真实重试，圆内点整数计数仍与串行完全一致。
10. 重复 attempt 请求返回已完成结果；相同 ID 不同 payload 返回 409。
11. 重启 Master 后文件元数据恢复，仍能下载。
12. 所有副本不可用时清理 partial，保留已有目标文件。
13. 全部测试进程停止，临时目录自动清理。

原始结果：[integration-results.json](integration-results.json)。测试使用独立临时目录与空闲端口，不污染默认演示数据。

## Dashboard

`python -m scripts.dashboard_check`：**7 个页面均无异常、无后端错误提示**。实际加载的 Plotly 图数依次为 4、3、3、5、5、8、1。采用真实运行的后端、文件、已完成 Job 和 Benchmark，使用 Streamlit 官方 AppTest 执行；并在浏览器中实际检查集群页面、主题和拓扑。

已修复实际观察到的浅色 KPI 文字对比度问题，以及窄视口中 Worker 多行标签相互重叠的问题。页面测试不是所有手工操作组合的穷举；未声称在所有浏览器、全部窗口尺寸和 Ubuntu 上完成视觉回归。

原始结果：[dashboard-results.json](dashboard-results.json)。截图如提供，来自运行系统；没有生成伪造运行截图。

## 性能实验

两类任务各执行串行 + 1/2/3 Worker，每组 3 次，共 **24 次真实测量**。每个分布式结果均与串行完全一致。

| 任务 | Worker | 端到端均值（秒） | 加速比（串行纯计算 / E2E） |
|---|---:|---:|---:|
| Monte Carlo 1,000,000 samples | 1 | 4.686 | 0.0452 |
| Monte Carlo 1,000,000 samples | 2 | 3.339 | 0.0634 |
| Monte Carlo 1,000,000 samples | 3 | 3.025 | 0.0700 |
| WordCount 5 MiB | 1 | 3.659 | 0.0445 |
| WordCount 5 MiB | 2 | 2.642 | 0.0616 |
| WordCount 5 MiB | 3 | 2.718 | 0.0599 |

这里分布式明显慢于串行；WordCount 从 2 到 3 Worker 也没有更快。原始值全部保留，未做有利于加速曲线的删改。此次验证同时存在界面/测试活动，属于真实功能性性能实验，**不是安静环境中的最终性能论文测量**。正式报告应关闭其他负载、增大工作量并重复实验。

原始配置、环境、每次测量、标准差和资源记录：

- [Monte Carlo](benchmark-monte-carlo.json)
- [WordCount](benchmark-wordcount.json)
- [依赖版本](environment.json)

## 启动、停止与未验证项

Windows `stop.ps1` 与 `start.ps1` 已真实执行；五个服务进程身份核验成功，重新启动后 3 Worker 注册、Dashboard 健康检查通过。交付前停止本项目全部进程；数据和结果保留，运行 start.ps1 可重新展示。

未运行：Ubuntu 实机验证、100 MiB/5000 万样本完整性能矩阵、生产级压力/长期运行测试、Master HA、自动副本修复。未把这些项目记作通过。
