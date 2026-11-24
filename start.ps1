$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
if (!(Test-Path '.venv/Scripts/python.exe')) {
    python -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw 'Python 3.11+ is required' }
}
& ./.venv/Scripts/python.exe -c "import fastapi, uvicorn, httpx, pydantic, psutil, streamlit, plotly, pandas"
if ($LASTEXITCODE -ne 0) {
    & ./.venv/Scripts/python.exe -m pip install -r requirements.txt
    if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed' }
}
& ./.venv/Scripts/python.exe -m scripts.start_cluster @args
if ($LASTEXITCODE -ne 0) { throw 'Cluster startup failed; see data/logs' }

