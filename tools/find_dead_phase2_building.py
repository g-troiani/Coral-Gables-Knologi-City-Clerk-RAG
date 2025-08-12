import json, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PHASE2 = ROOT / "scripts" / "graph_rag_stages" / "phase2_building"

# 1) collect all python files in phase2_building
files = sorted(p for p in PHASE2.rglob("*.py"))

# 2) scan repo for references to each module name (rough static)
repo_text = "\n".join(f.read_text(encoding="utf-8", errors="ignore") for f in ROOT.rglob("*.py"))

def refcount(path: Path) -> int:
    mod = ".".join(path.relative_to(ROOT).with_suffix("").parts)
    base = path.stem
    # count by module path and base name (class refs are harder, this is indicative)
    pattern = re.compile(rf"(\b{re.escape(mod)}\b|\b{re.escape(base)}\b)")
    return len(pattern.findall(repo_text))

# 3) parse latest debug log if present
logs = sorted((ROOT / "logs").glob("pipeline_run_*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
imports, calls, files_used = set(), set(), set()
if logs:
    last = logs[0].read_text(encoding="utf-8", errors="ignore")
    for line in last.splitlines():
        if "🔍 [IMPORT]" in line:
            imports.add(line)
        if "🎯 [CALL]" in line:
            calls.add(line)
        if "📁 [FILE]" in line:
            files_used.add(line)

print("\n=== Phase2 Building Dead-Code Candidates ===\n")
for f in files:
    cnt = refcount(f)
    seen = any(str(f) in L for L in (imports, files_used))
    print(f"- {f.relative_to(ROOT)} :: refs={cnt} :: runtime_seen={seen}")
