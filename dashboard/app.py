import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx
import streamlit as st
from dashboard.api_client import API
from dashboard import overview, storage, jobs, workloads, benchmarks, faults
from config.settings import MASTER

st.set_page_config(page_title='MiniGFS + MiniMapReduce', page_icon='◈', layout='wide')
st.sidebar.title('◈ MiniGFS')
st.sidebar.caption('DISTRIBUTED SYSTEMS LAB\n\nMiniMapReduce · 教学实验平台')
pages = {'集群总览': overview.render, '分布式文件管理': storage.render, 'MapReduce 任务中心': jobs.render,
         'WordCount 可视化': workloads.wordcount, 'Monte Carlo π': workloads.monte_carlo,
         '性能分析': benchmarks.render, '故障容错演示': faults.render}
page = st.sidebar.radio('工作空间', list(pages))
automatic = st.sidebar.toggle('每 3 秒自动刷新', value=True)
st.sidebar.button('立即刷新')
dark = st.sidebar.toggle('深色模式', key='dark')
st.sidebar.caption('Master: '+MASTER)
st.sidebar.link_button('后端 Swagger API', MASTER+'/docs')
background, foreground, card = ('#0f1729', '#ecf2ff', '#1b2940') if dark else ('#f5f8fd', '#193354', '#ffffff')
st.markdown(f'''<style>
.stApp {{background:{background};color:{foreground};}}
[data-testid="stSidebar"] {{background:{card};}}
[data-testid="stMetric"] {{background:{card};border:1px solid #7796c333;border-radius:12px;padding:16px;}}
[data-testid="stMetric"] * {{color:{foreground};}}
[data-testid="stHeader"] {{background:{background};}}
[data-testid="stSidebar"] p, [data-testid="stSidebar"] h1 {{color:{foreground};}}
.stMarkdown p, h1, h2, h3, [data-testid="stWidgetLabel"] p {{color:{foreground};}}
h1,h2,h3 {{letter-spacing:-.02em;}}
.block-container {{padding-top:2.2rem;max-width:1500px;}}
div.stButton>button[kind="primary"] {{background:#2879ed;border:none;}}
</style>''', unsafe_allow_html=True)


@st.fragment(run_every=3 if automatic else None)
def content():
    try:
        pages[page](API())
    except httpx.HTTPStatusError as error:
        st.error(f'后端请求失败 {error.response.status_code}: {error.response.text}')
    except httpx.RequestError as error:
        st.error('无法连接后端，请运行启动脚本。'+str(error))
    except (OSError, ValueError) as error:
        st.error(str(error))


content()
