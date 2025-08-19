import re
import sys
import subprocess
from typing import List

PATTERNS: List[re.Pattern] = [
    re.compile(r"ASANA_ACCESS_TOKEN\s*[:=]\s*['\"][A-Za-z0-9/_\-\.+=]{20,}['\"]"),
    re.compile(r"ASANA_CLIENT_(ID|SECRET)\s*[:=]\s*['\"][^'\"]{10,}['\"]"),
    re.compile(r"-----BEGIN (?:RSA|EC|OPENSSH) PRIVATE KEY-----"),
    re.compile(r"AIza[0-9A-Za-z\-_]{35}"),  # Google API Key
]


def run(cmd: str) -> str:
    return subprocess.check_output(cmd, shell=True, text=True)


def main() -> int:
    try:
        commits = run("git rev-list --all").strip().splitlines()
    except subprocess.CalledProcessError as e:
        print(f"git error: {e}")
        return 1

    leaks = []
    for c in commits:
        try:
            diff = run(f"git show {c} -- . ':(exclude)*.png' ':(exclude)*.jpg' ':(exclude)*.pdf'")
        except subprocess.CalledProcessError:
            continue
        for pat in PATTERNS:
            for m in pat.finditer(diff):
                leaks.append((c, m.group(0)[:200]))
    if not leaks:
        print("No obvious secrets found in git history by simple patterns.")
        return 0
    print("Potential secrets found:")
    for c, frag in leaks:
        print(f"- commit {c}: {frag}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
