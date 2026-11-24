$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
& ./.venv/Scripts/python.exe -m scripts.stop_cluster
if ($LASTEXITCODE -ne 0) { throw 'Cluster shutdown failed' }

