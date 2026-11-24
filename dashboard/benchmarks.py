import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dashboard.jobs import parameters
from dashboard.components import chart, export_json


def render(api):
    st.header('性能分析')
    st.caption('Performance Benchmark · 固定工作量 · 至少三次重复 · 保留全部真实结果')
    with st.expander('配置并启动实验'):
        spec = parameters(api, 'bench')
        online = sum(w['online'] for w in api.get('/workers'))
        counts = st.multiselect('并行度组', list(range(1, online+1)), default=list(range(1, min(online, 3)+1)))
        repeats = st.number_input('每组重复次数', 3, 20, 3)
        if st.button('运行串行及分布式实验', type='primary', disabled=not counts):
            benchmark = api.post('/benchmarks', {**spec, 'worker_counts': counts, 'repeats': repeats})
            st.success('已开始：'+benchmark['benchmark_id'])
    benchmarks = api.get('/benchmarks')
    if not benchmarks:
        st.info('尚无实验记录。首次小规模运行会包含进程启动开销，可能没有加速。')
        return
    lookup = {b['benchmark_id']: b for b in reversed(benchmarks)}
    selected = st.selectbox('历史实验', list(lookup), format_func=lambda x: x[:10]+' · '+lookup[x]['status'])
    benchmark = lookup[selected]
    st.write('状态：', benchmark['status'])
    if benchmark.get('error'):
        st.error(benchmark['error'])
    rows = pd.DataFrame(benchmark['rows'])
    if not rows.empty:
        st.dataframe(rows, hide_index=True, width='stretch')
        st.download_button('原始测量 CSV', rows.to_csv(index=False).encode('utf-8-sig'), 'benchmark.csv', 'text/csv')
    summary = pd.DataFrame(benchmark['summary'])
    if not summary.empty:
        left, right = st.columns(2)
        with left:
            chart(px.line(summary, x='workers', y='mean', error_y='stdev', markers=True, title='Worker 数量与端到端耗时 · 均值 ± 标准差'))
        with right:
            figure = px.line(summary, x='workers', y='speedup', markers=True, title='实际加速比与理想线性加速比')
            figure.add_trace(go.Scatter(x=summary['workers'], y=summary['workers'], name='理想线性加速', line=dict(dash='dash')))
            chart(figure)
        left, right = st.columns(2)
        with left:
            chart(px.line(summary, x='workers', y='efficiency', markers=True, title='并行效率 · %'))
        with right:
            comparison = pd.DataFrame([{'configuration': '串行纯计算', 'seconds': summary.iloc[0]['serial_compute']}] +
                                     [{'configuration': f"{int(r['workers'])} Workers E2E", 'seconds': r['mean']} for r in benchmark['summary']])
            chart(px.bar(comparison, x='configuration', y='seconds', title='串行基准与分布式端到端时间'))
        distributed = rows[rows['workers'] > 0]
        melted = distributed.groupby('workers', as_index=False)[['map', 'shuffle', 'reduce', 'coordination']].mean().melt(id_vars='workers', var_name='phase', value_name='seconds')
        chart(px.bar(melted, x='workers', y='seconds', color='phase', title='阶段耗时（Map / Reduce 已包含队列等待及网络）'))
        chart(px.box(distributed, x='workers', y='seconds', points='all', title='全部重复实验的耗时分布'))
        st.dataframe(summary, hide_index=True, width='stretch')
        best = summary.loc[summary['mean'].idxmin()]
        st.info(f"本次最短平均时间为 {int(best['workers'])} Workers：{best['mean']:.3f} 秒；相对串行纯计算加速比 {best['speedup']:.2f}×。" +
                (' 本次分布式执行更慢；任务规模、轮询、通信与进程启动开销均可能影响结果。' if best['speedup'] < 1 else ' 更多 Worker 是否有收益应以重复测量为准。'))
    history = pd.DataFrame(benchmark['resource_history'])
    if not history.empty:
        chart(px.line(history, x='time', y='cpu', color='workers', title='实验期间主机 CPU · %'))
        chart(px.line(history, x='time', y='memory_bytes', color='workers', title='实验期间 Worker 进程树内存 · bytes'))
    others = st.multiselect('比较已完成历史实验', [k for k, b in lookup.items() if b['summary']], default=[selected] if benchmark['summary'] else [])
    compare = [dict(row, experiment=key[:8]) for key in others for row in lookup[key]['summary']]
    if len(others) > 1:
        chart(px.line(pd.DataFrame(compare), x='workers', y='mean', color='experiment', markers=True, title='历史配置比较（注意工作量差异）'))
    with st.expander('实验配置、环境与计时口径'):
        st.json(benchmark['configuration'])
        st.json(benchmark['environment'])
        st.write('Speedup = 串行纯计算均值 / 分布式端到端均值。compute_seconds 为任务计算秒数之和，可能大于墙钟时间；coordination 仅为阶段外调度时间。transfer_bytes 为任务请求、结果和远程 Chunk 的应用载荷，不含协议头、轮询和上传。首轮冷启动计入且不剔除。')
    export_json('完整实验 JSON', benchmark, 'benchmark.json')

