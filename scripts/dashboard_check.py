"""Streamlit's official AppTest executes each page against a real running backend."""
import json
from pathlib import Path
from streamlit.testing.v1 import AppTest
from config.settings import ROOT


def run():
    app = AppTest.from_file(str(ROOT / 'dashboard' / 'app.py'), default_timeout=45).run()
    assert not app.exception, [e.message for e in app.exception]
    results = []
    pages = ['集群总览', '分布式文件管理', 'MapReduce 任务中心', 'WordCount 可视化', 'Monte Carlo π', '性能分析', '故障容错演示']
    for page in pages:
        app.sidebar.radio[0].set_value(page).run()
        errors = [e.message for e in app.exception]
        assert not errors, (page, errors)
        assert not app.error, (page, [e.value for e in app.error])
        results.append({'page': page, 'exceptions': 0, 'plots': len(app.get('plotly_chart'))})
    (ROOT/'docs'/'dashboard-results.json').write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    run()
