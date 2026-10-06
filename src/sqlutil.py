"""Small helper shared by tests and new scripts: load a .sql file, drop comment lines, fill placeholders, split into statements."""
from config import ROOT

def statements(name, **kw):
    raw = (ROOT / "sql" / name).read_text()
    text = "\n".join(l for l in raw.splitlines() if not l.strip().startswith("--")).format(**kw)
    return [s.strip() for s in text.split(";") if s.strip()]
