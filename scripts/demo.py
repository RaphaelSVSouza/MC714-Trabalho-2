from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = [
    "scripts/smoke_http.py",
    "scripts/smoke_lamport.py",
    "scripts/smoke_mutex.py",
    "scripts/smoke_election.py",
]


def run(script: str) -> int:
    print(f"\n==> {script}")
    result = subprocess.run([sys.executable, script], cwd=ROOT)
    return result.returncode


if __name__ == "__main__":
    for script in SCRIPTS:
        code = run(script)
        if code != 0:
            raise SystemExit(code)
    raise SystemExit(0)
