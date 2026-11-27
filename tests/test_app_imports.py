"""The hosted app installs only requirements/app.txt (Flask and gunicorn).

Importing the server must therefore not pull in numpy, pandas or the other
pipeline libraries.
"""
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

_CHECK = """
import sys
class Block:
    def find_spec(self, name, path=None, target=None):
        if name.split('.')[0] in {'numpy', 'pandas', 'sklearn', 'scipy', 'plotly', 'shap'}:
            raise ImportError('not installed in the app image: ' + name)
sys.meta_path.insert(0, Block())
sys.path.insert(0, sys.argv[1])
import app.server
import src.agent
"""


def test_server_imports_without_pipeline_libraries():
    result = subprocess.run([sys.executable, "-c", _CHECK, str(ROOT)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
