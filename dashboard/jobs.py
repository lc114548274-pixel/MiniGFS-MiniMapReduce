from datetime import datetime, timezone
import pandas as pd
import plotly.express as px
import streamlit as st
from dashboard.components import chart, export_json, gantt, kpis, logs, phase_chart, select_job


def parameters(api, key='job'):
    kind = st.radio('任务类型', ['wordcount', 'monte_carlo'], horizontal=True, key=key+'_kind')
    files = {f['file_id']: f for f in api.get('/files') if f['status'] == 'completed'}
    result = {'job_type': kind}
    if kind == 'wordcount':
        result['input_file_id'] = st.selectbox('输入文件', list(files), format_func=lambda x: files[x]['filename'], key=key+'_file') if files else None
        if not files:
            st.warning('先上传一个文件。')
    else:
        result['samples'] = int(st.number_input('总采样次数（预设：1M / 5M / 10M / 50M，也可自定义）', 1, 1000000000, 1000000, key=key+'_samples'))
        result['partitions'] = int(st.number_input('Map 分片数量', 1, 128, 6, key=key+'_parts'))
        result['seed'] = int(st.number_input('随机种子', 0, 2**32-1, 42, key=key+'_seed'))
    columns = st.columns(3)
    result['worker_count'] = columns[0].number_input('最多使用 Worker', 1, 32, 3, key=key+'_workers')
    result['max_concurrency'] = columns[1].number_input('每 Worker 并发上限', 1, 8, 1, key=key+'_slots')
    result['reducers'] = columns[2].number_input('Reduce 分区（WordCount）', 1, 32, 2, key=key+'_reduce')
    return result


def render(api):
    st.header('MapReduce 任务中心')
    with st.expander('新建计算任务', expanded=True):
        spec = parameters(api)
        if st.button('提交 Job', type='primary'):
            job = api.post('/jobs', spec)
            st.success('已提交 ' + job['job_id'])
    job = select_job(api)
    if not job:
        return
    tasks = job['map_tasks'] + job['reduce_tasks']
    done = sum(t['status'] == 'completed' for t in tasks)
    st.subheader(f"{job['job_type']} · {job['status']}")
    st.code(job['job_id'])
    elapsed = job['execution_time'] if job['execution_time'] is not None else ((datetime.now(timezone.utc)-datetime.fromisoformat(job['started_at'])).total_seconds() if job['started_at'] else 0)
    st.caption(f"已执行 {elapsed:.2f} 秒 · {job['data_transfer_bytes']:,} bytes 计算数据传输")
    # Reducers are not allocated until shuffle, so use the expected total throughout.
    expected = len(job['map_tasks']) + (job['parameters']['reducers'] if job['job_type'] == 'wordcount' else 1)
    st.progress(done/max(1, expected), text=f'{done}/{expected} Tasks 完成')
    kpis([('Map 完成', sum(t['status'] == 'completed' for t in job['map_tasks'])),
          ('Reduce 完成', sum(t['status'] == 'completed' for t in job['reduce_tasks'])),
          ('运行', sum(t['status'] == 'running' for t in tasks)), ('失败', sum(t['status'] == 'failed' for t in tasks)),
          ('重试', sum(max(0, t['attempt']-1) for t in tasks))])
    phase_chart(job)
    gantt(job)
    if job.get('error'):
        st.error(job['error'])
    if job['phases']:
        chart(px.bar(x=list(job['phases']), y=list(job['phases'].values()), labels={'x': '阶段', 'y': '秒'}, title='阶段墙钟时间'))
    st.dataframe(pd.DataFrame([{k: t.get(k) for k in ['task_id', 'task_type', 'chunk_id', 'assigned_worker', 'status', 'attempt', 'execution_time', 'error_message']} for t in tasks]), hide_index=True, width='stretch')
    if job['status'] == 'completed':
        export_json('导出计算结果 JSON', job['result'], job['job_id']+'.json')
    logs(api, job['job_id'])

