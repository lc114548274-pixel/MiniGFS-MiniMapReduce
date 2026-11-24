import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dashboard.components import chart, kpis, logs


def render(api):
    st.header('集群总览')
    st.caption('Cluster Overview · 独立进程 / HTTP 通信 / 实时心跳')
    metrics, workers = api.get('/metrics'), api.get('/workers')
    kpis([('Worker 总数', metrics['worker_count']), ('在线', metrics['online']), ('离线', metrics['offline']), ('运行任务', metrics['running_tasks'])])
    kpis([('文件', metrics['files']), ('Chunks', metrics['chunks']), ('已存储副本', metrics['replicas']),
          ('存储 MiB', f"{metrics['storage_bytes']/2**20:.2f}"), ('完成 Jobs', metrics['completed_jobs'])])
    st.subheader('集群拓扑 · Live topology')
    fig = go.Figure()
    center = (len(workers)-1)/2
    for i, worker in enumerate(workers):
        fig.add_trace(go.Scatter(x=[center, i], y=[1, 0], mode='lines', line=dict(color='#c6d4ea', width=2), hoverinfo='skip', showlegend=False))
    fig.add_trace(go.Scatter(x=[center], y=[1], mode='markers+text', marker=dict(size=40, color='#2879ed'),
                             text=['MASTER'], textposition='top center', showlegend=False))
    for i, worker in enumerate(workers):
        label = f"{worker['worker_id']}<br>{'Online' if worker['online'] else 'Offline'}<br>CPU {worker['cpu']:.1f}%<br>RAM {worker['memory']/2**20:.0f} MiB<br>Disk {worker['used']/2**20:.1f} MiB<br>Tasks {worker['tasks']}"
        fig.add_trace(go.Scatter(x=[i], y=[0], mode='markers+text', marker=dict(size=32, color='#16a577' if worker['online'] else '#e45b68'), text=[label], textfont=dict(size=12), textposition='bottom center', showlegend=False))
    fig.update_layout(height=420, xaxis=dict(visible=False, range=[-.65, max(1, len(workers)-.35)]), yaxis=dict(visible=False, range=[-1.1, 1.5]))
    chart(fig)
    if workers:
        frame = pd.DataFrame(workers)
        frame['storage_mib'] = frame['used']/2**20
        frame['disk_percent'] = frame['used']/frame['capacity']*100
        chart(px.bar(frame, x='worker_id', y='storage_mib', color='online', title='Worker 项目存储占用 · MiB', hover_data=['disk_percent']))
    rows = [{'time': record['time'], 'worker': w['worker_id'], 'cpu': w['cpu'], 'memory_mib': w['memory']/2**20}
            for record in metrics['history'] for w in record['workers'] if w['online']]
    if rows:
        frame = pd.DataFrame(rows)
        left, right = st.columns(2)
        with left:
            chart(px.line(frame, x='time', y='cpu', color='worker', title='主机 CPU 使用率 · %'))
        with right:
            chart(px.line(frame, x='time', y='memory_mib', color='worker', title='Worker 进程树内存 · MiB'))
        st.caption('同机 Worker 的 CPU 指标为主机整体利用率，不能相加；内存包含各 Worker 子进程。历史最多保留 1 小时。')
    logs(api)

