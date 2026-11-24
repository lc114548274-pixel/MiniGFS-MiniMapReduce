import pandas as pd
import streamlit as st
from dashboard.components import gantt, kpis, logs, select_job


def render(api):
    st.header('故障容错演示')
    st.caption('Fault Tolerance · 操作真实本地进程 · 仅管理启动器登记的 Worker')
    records = [r for r in api.get('/processes') if r['name'].startswith('worker-')]
    workers = {w['worker_id']: w for w in api.get('/workers')}
    if not records:
        st.info('请使用项目启动脚本创建 Worker 后再操作。')
    for record in records:
        columns = st.columns([3, 1, 1, 1])
        worker = workers.get(record['name'], {})
        columns[0].write(f"**{record['name']}** · PID {record['pid']} · 进程 {'运行' if record['alive'] else '停止'} · 心跳 {'Online' if worker.get('online') else 'Offline'}")
        for column, action, label in zip(columns[1:], ['start', 'stop', 'restart'], ['启动', '停止', '重启']):
            if column.button(label, key=record['name']+action):
                result = api.post(f"/processes/{record['name']}/{action}")
                st.success(f"{record['name']} {label} 已执行")
                st.json(result)
    st.caption('默认心跳 3 秒，超过 10 秒标记 Offline；任务连接失败可能更早触发重试。')
    files = api.get('/files')
    chunks = [c for f in files for c in f['chunks']]
    kpis([('降级 Chunks', sum(c['health'] == 'Degraded' for c in chunks)),
          ('不可用 Chunks', sum(c['health'] == 'Unavailable' for c in chunks)),
          ('受离线副本影响', sum(any(not r['online'] for r in c['replicas']) for c in chunks))])
    with st.expander('课堂演示步骤', expanded=True):
        st.markdown('1. 上传超过 4 MiB 的文件，观察双副本矩阵。\n2. 停止一个 Worker，等待心跳超时。\n3. 在文件页下载并验证 SHA-256。\n4. 重启 Worker，提交 5000 万次 Monte Carlo 任务。\n5. 在任务为 running 时停止其中一个 Worker，观察重试和最终结果。')
    job = select_job(api)
    if job:
        tasks = job['map_tasks'] + job['reduce_tasks']
        retries = sum(max(0, t['attempt']-1) for t in tasks)
        st.write(f"最终状态：{job['status']} · 重试次数：{retries}")
        recovery = []
        for task in tasks:
            failed = [a for a in task['attempts'] if a['status'] == 'failed']
            successful = [a for a in task['attempts'] if a['status'] == 'completed']
            if failed and successful:
                seconds = (pd.Timestamp(successful[-1]['finished_at'])-pd.Timestamp(failed[0]['finished_at'])).total_seconds()
                recovery.append({'task': task['task_id'], 'from': failed[0]['worker_id'], 'to': successful[-1]['worker_id'], 'recovery_seconds': seconds})
        if recovery:
            st.dataframe(pd.DataFrame(recovery), hide_index=True, width='stretch')
        gantt(job)
    events = api.get('/events')
    timeline = [e for e in events if e['event'] in ['worker_online', 'worker_offline', 'task_retry', 'job_completed', 'job_failed', 'download_verified']]
    st.subheader('故障与恢复时间线')
    st.dataframe(pd.DataFrame(timeline[::-1]), hide_index=True, width='stretch')
    logs(api)

