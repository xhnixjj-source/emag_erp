"""One-off utility: remove Cursor agent instrumentation blocks from Python sources."""
from pathlib import Path

REGION_START = "# #region agent log"
REGION_END = "# #endregion"


def strip_agent_regions(content: str) -> str:
    lines = content.splitlines(keepends=True)
    out = []
    i = 0
    n = len(lines)
    while i < n:
        if REGION_START in lines[i]:
            i += 1
            while i < n and REGION_END not in lines[i]:
                i += 1
            if i < n:
                i += 1
            continue
        out.append(lines[i])
        i += 1
    return "".join(out)


def main():
    root = Path(__file__).resolve().parent.parent
    for p in root.rglob("*.py"):
        t = p.read_text(encoding="utf-8")
        if REGION_START not in t:
            continue
        new = strip_agent_regions(t)
        if new != t:
            p.write_text(new, encoding="utf-8")
            print("stripped regions:", p.relative_to(root.parent))


if __name__ == "__main__":
    main()
