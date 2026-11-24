import math
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dashboard.components import chart, kpis, select_job, export_json


def task_charts(job):
    frame = pd.DataFrame([{'worker': t['assigned_worker'], 'seconds': t['execution_time'] or 0,
                           'bytes': t.get('input_bytes', 0), 'task': t['task_id'][:8]} for t in job['map_tasks']])
    if not frame.empty:
        left, right = st.columns(2)
        with left:
            chart(px.bar(frame, x='task', y='seconds', color='worker', title='各 Map Task 计算时间'))
        with right:
            chart(px.bar(frame.groupby('worker', as_index=False)['bytes'].sum(), x='worker', y='bytes', title='Worker 处理输入字节数'))
    chart(px.bar(x=list(job['phases']), y=list(job['phases'].values()), labels={'x': '阶段', 'y': '秒'}, title='Map / Shuffle / Reduce 时间分解'))


def wordcount(api):
    st.header('WordCount 可视化')
    job = select_job(api, 'wordcount', completed=True)
    if not job:
        return
    counts = job['result']
    file = api.get('/files/'+job['input_file_id'])
    kpis([('总单词', sum(counts.values())), ('不同单词', len(counts)), ('输入 MiB', f"{file['file_size']/2**20:.2f}"), ('耗时 s', f"{job['execution_time']:.3f}")])
    st.caption(f"{len(job['map_tasks'])} Map Tasks · {len(job['reduce_tasks'])} Reduce Tasks · 英文字母连续序列，忽略大小写")
    frame = pd.DataFrame(sorted(counts.items(), key=lambda p: (-p[1], p[0])), columns=['word', 'count'])
    if not frame.empty:
        left, right = st.columns(2)
        with left:
            chart(px.bar(frame.head(20).iloc[::-1], x='count', y='word', orientation='h', title='Top 20 高频单词'))
        with right:
            chart(px.histogram(frame, x='count', nbins=30, title='词频分布 · 每个单词的出现次数'))
    task_charts(job)
    search = st.text_input('搜索单词')
    st.dataframe(frame[frame['word'].str.contains(search, regex=False)] if search else frame, hide_index=True, width='stretch')
    st.download_button('下载完整 CSV', frame.to_csv(index=False).encode('utf-8-sig'), 'wordcount.csv', 'text/csv')
    with st.expander('Map → Shuffle → Reduce 中间数据'):
        if st.button('读取原始输入示例（前 2 KiB）'):
            st.code(api.get('/files/'+job['input_file_id']+'/preview')['text'], language='text')
        st.caption('本地 Combiner 后的 Map 计数、真实 Shuffle 分组和最终输出（各预览最多 30 项）。')
        st.json({key: dict(list(value.items())[:30]) for key, value in list(job['intermediates'].items())[:5]})
        st.json(dict(list(job['shuffle_groups'].items())[:30]))
        st.json(dict(list(counts.items())[:30]))


def monte_carlo(api):
    st.header('Monte Carlo π')
    st.caption('独立随机种子 · CPU 进程池 · 可复现真实采样')
    with st.expander('快速提交 π 实验'):
        samples = st.number_input('采样次数', 1, 1000000000, 1000000)
        parts = st.number_input('任务数量', 1, 128, 6)
        workers = st.number_input('Worker 数量', 1, 32, 3)
        seed = st.number_input('Seed', 0, 2**32-1, 42)
        if st.button('开始采样', type='primary'):
            job = api.post('/jobs', dict(job_type='monte_carlo', samples=samples, partitions=parts, worker_count=workers, seed=seed))
            st.success('任务已提交：'+job['job_id'])
    job = select_job(api, 'monte_carlo', completed=True)
    if not job:
        return
    result = job['result']
    estimate, error = result['pi'], abs(result['pi']-math.pi)
    kpis([('真实 π', f'{math.pi:.8f}'), ('估算 π', f'{estimate:.8f}'), ('绝对误差', f'{error:.6f}'), ('相对误差', f'{error/math.pi*100:.4f}%')])
    kpis([('采样总量', result['total_samples']), ('圆内点', result['inside_circle']), ('执行秒数', f"{job['execution_time']:.3f}")])
    points = pd.DataFrame(result['points'], columns=['x', 'y', 'inside'])
    left, right = st.columns(2)
    with left:
        fig = px.scatter(points, x='x', y='y', color='inside', color_discrete_map={True: '#2879ed', False: '#eea332'}, title='单位正方形内的真实采样点')
        xs = [i/100 for i in range(101)]
        fig.add_trace(go.Scatter(x=xs, y=[math.sqrt(1-x*x) for x in xs], mode='lines', line=dict(color='#16a577'), name='单位圆'))
        fig.update_xaxes(range=[0, 1])
        fig.update_yaxes(range=[0, 1], scaleanchor='x', scaleratio=1)
        chart(fig)
        st.caption(f"仅展示 {len(points)} 个真实样本点；不保存全部随机点。")
    rows, total, inside = [], 0, 0
    for batch in result['batches']:
        total += batch['samples']
        inside += batch['inside_circle']
        rows.append({'samples': total, 'pi': 4*inside/total, 'absolute_error': abs(4*inside/total-math.pi)})
    frame = pd.DataFrame(rows)
    with right:
        fig = px.line(frame, x='samples', y='pi', title='真实批次累计 π 估计')
        fig.add_hline(y=math.pi, line_dash='dash', annotation_text='π')
        chart(fig)
        st.caption('按固定分片顺序累加真实批次计数，并非各进程实时完成顺序。')
    chart(px.line(frame, x='samples', y='absolute_error', title='绝对误差随采样数量变化'))
    rows = [{'worker': t['assigned_worker'], 'samples': job['intermediates'][t['task_id']]['total_samples'],
             'seconds': t['execution_time']} for t in job['map_tasks']]
    frame = pd.DataFrame(rows).groupby('worker', as_index=False).sum()
    left, right = st.columns(2)
    with left:
        chart(px.bar(frame, x='worker', y='samples', title='各 Worker 采样数量'))
    with right:
        chart(px.bar(frame, x='worker', y='seconds', title='各 Worker 累计计算时间'))
    export_json('导出 π 结果及真实采样批次', result, 'monte-carlo.json')

