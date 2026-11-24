from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd
import plotly.express as px
import streamlit as st
from config.settings import DATA
from dashboard.components import chart, kpis


def render(api):
    st.header('分布式文件管理')
    st.caption('Distributed Storage · 4 MiB chunks · SHA-256 · 双副本')
    with st.form('upload'):
        upload = st.file_uploader('选择普通文件（文本、CSV、二进制）')
        submit = st.form_submit_button('上传至集群', type='primary')
    if submit and upload:
        DATA.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(dir=DATA) as directory:
            path = Path(directory) / Path(upload.name).name
            path.write_bytes(upload.getvalue())
            progress = st.progress(0.0)
            file = api.upload(path, progress=progress.progress)
        if file['status'] == 'completed':
            st.success('上传完成；成功副本已由 Master 回读校验。')
        else:
            st.error('上传未完成，至少一个 Chunk 没有成功副本。')
        for warning in file['warnings']:
            st.warning(warning)
    files = api.get('/files')
    if not files:
        st.info('尚无文件。上传后可查看真实副本分布。')
        return
    rows = [{k: f[k] for k in ['filename', 'file_id', 'file_size', 'status', 'created_at']} |
            {'chunks': len(f['chunks']), 'stored_replicas': sum(r['status'] == 'stored' for c in f['chunks'] for r in c['replicas'])} for f in files]
    st.dataframe(pd.DataFrame(rows), hide_index=True, width='stretch')
    lookup = {f['file_id']: f for f in files}
    selected = st.selectbox('查看文件', list(lookup), format_func=lambda key: f"{lookup[key]['filename']} · {key[:8]}")
    file = lookup[selected]
    health = [c['health'] for c in file['chunks']]
    kpis([('Healthy', health.count('Healthy')), ('Degraded', health.count('Degraded')), ('Unavailable', health.count('Unavailable'))])
    st.caption('副本 stored 是持久化状态；健康度同时考虑节点是否在线。')
    if file['chunks']:
        chunk_frame = pd.DataFrame([dict(index=c['index'], bytes=c['size'], group='File') for c in file['chunks']])
        chart(px.bar(chunk_frame, x='bytes', y='group', color='index', orientation='h', title='文件切块结构 · 字节顺序'))
        workers = sorted({r['worker_id'] for c in file['chunks'] for r in c['replicas']})
        matrix, detail = [], []
        for chunk in file['chunks']:
            replicas = {r['worker_id']: r for r in chunk['replicas']}
            matrix.append([2 if w in replicas and replicas[w]['status'] == 'stored' and replicas[w]['online'] else
                           1 if w in replicas and replicas[w]['status'] == 'stored' else 0 for w in workers])
            detail.append(dict(index=chunk['index'], chunk_id=chunk['chunk_id'], size=chunk['size'], sha256=chunk['checksum'],
                               health=chunk['health'], replicas='; '.join(f"{r['worker_id']}: {r['status']} / {'Online' if r['online'] else 'Offline'}" for r in chunk['replicas'])))
        chart(px.imshow(matrix, x=workers, y=[f"Chunk {c['index']}" for c in file['chunks']], zmin=0, zmax=2,
                        text_auto=True, color_continuous_scale=['#e9eef7', '#eea332', '#16a577'], title='副本矩阵 · 0 无成功副本 / 1 离线副本 / 2 在线副本', aspect='auto'))
        chart(px.bar(x=workers, y=[sum(r['worker_id'] == w and r['status'] == 'stored' for c in file['chunks'] for r in c['replicas']) for w in workers], labels={'x': 'Worker', 'y': 'Chunks'}, title='各 Worker 已存储 Chunk 数'))
        st.dataframe(pd.DataFrame(detail), hide_index=True, width='stretch')
    if st.button('下载并校验 SHA-256', disabled=file['status'] != 'completed'):
        with TemporaryDirectory(dir=DATA) as directory:
            target = Path(directory) / 'download.bin'
            report = api.download(selected, target)
            st.session_state['download'] = {'file_id': selected, 'data': target.read_bytes(), 'report': report}
    download = st.session_state.get('download')
    if download and download['file_id'] == selected:
        st.success('完整性校验通过 · ' + download['report']['sha256'])
        st.json(download['report'])
        st.download_button('保存恢复文件', download['data'], file['filename'])

