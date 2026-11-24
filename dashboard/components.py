import json
from datetime import datetime, timezone
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

COLORS = {'completed': '#16a577', 'online': '#16a577', 'stored': '#16a577', 'running': '#2879ed',
          'mapping': '#2879ed', 'reducing': '#2879ed', 'shuffling': '#eea332', 'retrying': '#eea332',
          'pending': '#9aa7b9', 'failed': '#e45b68', 'offline': '#e45b68'}


def chart(fig, key=None):
    fig.update_layout(template='plotly_dark' if st.session_state.get('dark') else 'plotly_white',
                      font=dict(family='Segoe UI, Microsoft YaHei, sans-serif', size=14),
                      margin=dict(l=20, r=20, t=45, b=25), colorway=['#2879ed', '#16a577', '#eea332', '#9d7bea'])
    st.plotly_chart(fig, width='stretch', key=key, config={'displaylogo': False, 'toImageButtonOptions': {'format': 'png'}})


def kpis(values):
    columns = st.columns(len(values))
    for column, (label, value) in zip(columns, values):
        column.metric(label, value)


def export_json(label, value, filename):
    st.download_button(label, json.dumps(value, ensure_ascii=False, indent=2), filename, 'application/json')


def phase_chart(job):
    phases = ['pending', 'mapping', 'shuffling', 'reducing', 'completed']
    labels = ['Input / Chunks', 'Map Tasks', 'Shuffle / Partition', 'Reduce Tasks', 'Final Output']
    current = phases.index(job['status']) if job['status'] in phases else -1
    colors = [COLORS['failed'] if current == -1 else COLORS['completed'] if i < current else
              COLORS.get(job['status'], '#2879ed') if i == current else COLORS['pending'] for i in range(5)]
    figure = go.Figure(go.Scatter(x=list(range(5)), y=[0]*5, mode='lines+markers+text',
        line=dict(color='#c6d4ea', width=4), marker=dict(size=38, color=colors), text=labels, textposition='bottom center'))
    figure.update_layout(height=180, xaxis=dict(visible=False, range=[-.4, 4.4]), yaxis=dict(visible=False, range=[-.5, .5]))
    chart(figure)


def gantt(job):
    rows = []
    for task in job['map_tasks'] + job['reduce_tasks']:
        for attempt in task['attempts']:
            rows.append({'task': task['task_type'] + ' · ' + task['task_id'][:7], 'worker': attempt['worker_id'],
                         'start': attempt['started_at'], 'end': attempt.get('finished_at') or datetime.now(timezone.utc).isoformat(),
                         'status': attempt['status'], 'attempt': attempt['attempt']})
    if rows:
        frame = pd.DataFrame(rows)
        frame['start'], frame['end'] = pd.to_datetime(frame['start']), pd.to_datetime(frame['end'])
        figure = px.timeline(frame, x_start='start', x_end='end', y='task', color='status',
                             color_discrete_map=COLORS, hover_data=['worker', 'attempt'], title='真实任务尝试时间线 · Gantt')
        figure.update_yaxes(autorange='reversed')
        chart(figure)
        with st.expander('任务重试与分配记录'):
            st.dataframe(frame, hide_index=True, width='stretch')
    else:
        st.info('任务开始后显示真实时间线。')


def logs(api, job_id=None):
    with st.expander('系统事件 · Event Log', expanded=False):
        events = api.get('/events')
        if job_id:
            events = [e for e in events if e.get('job_id') == job_id]
        if not events:
            st.caption('暂无事件')
            return
        columns = st.columns(4)
        filters = {}
        for column, field, label in zip(columns, ['level', 'worker_id', 'job_id', 'event'], ['级别', 'Worker', 'Job', '事件类型']):
            filters[field] = column.selectbox(label, ['全部'] + sorted({str(e[field]) for e in events if e.get(field)}), key='log_'+field)
        events = [e for e in events if all(v == '全部' or str(e.get(k)) == v for k, v in filters.items())]
        st.dataframe(pd.DataFrame(events[::-1]), hide_index=True, width='stretch')


def select_job(api, kind=None, completed=False):
    jobs = [j for j in api.get('/jobs') if (not kind or j['job_type'] == kind) and (not completed or j['status'] == 'completed')]
    if not jobs:
        st.info('暂无符合条件的任务。请先在任务中心提交。')
        return None
    lookup = {j['job_id']: j for j in reversed(jobs)}
    selected = st.selectbox('选择 Job', list(lookup), format_func=lambda key: f"{key[:10]} · {lookup[key]['job_type']} · {lookup[key]['status']}")
    return api.get('/jobs/' + selected)

