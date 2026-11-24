"""Package source, documentation and real validation artifacts, excluding runtime data."""
import os
import zipfile
from pathlib import Path
from config.settings import ROOT


def package() -> Path:
    excluded = {'.venv', 'data', '__pycache__', '.pytest_cache', '.git'}
    paths = []
    for directory, dirs, files in os.walk(ROOT):
        dirs[:] = [name for name in dirs if name not in excluded]
        for name in files:
            if not name.endswith('.pyc'):
                paths.append(Path(directory)/name)
    manifest = ROOT/'docs'/'FILES.txt'
    paths = sorted(set([*paths, manifest]))
    manifest.write_text('\n'.join(p.relative_to(ROOT).as_posix() for p in paths)+'\n', encoding='utf-8')
    archive = ROOT.parent/'MiniGFS-MiniMapReduce-source.zip'
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as output:
        for path in paths:
            output.write(path, ROOT.name+'/'+path.relative_to(ROOT).as_posix())
    with zipfile.ZipFile(archive) as output:
        assert output.testzip() is None
    print(f'{len(paths)} files; {archive.stat().st_size} bytes; {archive}')
    return archive


if __name__ == '__main__':
    package()
