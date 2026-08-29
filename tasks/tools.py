"""
Tool layer -- pluggable, with REAL and MOCK implementations behind one interface.

Every tool declares both. The active mode is data, not code:

    UPDATE runtime_config SET value='"live"' WHERE key='tool_mode';
    -- or per-tool:  POST /chaos/tool {"name":"github","mode":"live"}

WHY BOTH EXIST
--------------
Real calls make the system honest: `files`, `shell` and `http` below do actual
work. But a live third-party API during a demo is a dependency you do not
control -- a rate limit on stage looks like your bug. So mock is the default and
live is opt-in per tool.

Chaos (latency + injected failure) applies in BOTH modes. You must be able to
break a live tool too, or the fault-injection story only works on fakes.
"""

from __future__ import annotations

import json
import random
import subprocess
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from common.failures import InfraFailure, PoisonFailure
from db.pool import pool

REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass
class Tool:
    name: str
    mock: Callable[[dict], dict]
    live: Callable[[dict], dict] | None = None
    latency_ms: int = 200
    failure_rate: float = 0.0
    side_effecting: bool = False


TOOLS: dict[str, Tool] = {}


def tool(name: str, *, latency_ms: int = 200, failure_rate: float = 0.0,
         side_effecting: bool = False):
    """Register a tool's MOCK implementation."""
    def wrap(fn: Callable[[dict], dict]):
        TOOLS[name] = Tool(name, mock=fn, latency_ms=latency_ms,
                           failure_rate=failure_rate, side_effecting=side_effecting)
        return fn
    return wrap


def live_impl(name: str):
    """Attach the REAL implementation to an already-registered tool."""
    def wrap(fn: Callable[[dict], dict]):
        TOOLS[name].live = fn
        return fn
    return wrap


# --- runtime config (chaos knobs), cached briefly ----------------------------

_cfg: dict[str, Any] = {}
_cfg_at = 0.0
_lock = threading.Lock()
_TTL = 1.0


def _config() -> dict[str, Any]:
    global _cfg, _cfg_at
    with _lock:
        if time.time() - _cfg_at < _TTL:
            return _cfg
        try:
            with pool().connection() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT key, value FROM runtime_config "
                    "WHERE key IN ('tool_overrides','tool_mode')"
                )
                rows = {r["key"]: r["value"] for r in cur.fetchall()}
            _cfg = {
                "overrides": rows.get("tool_overrides") or {},
                "mode": rows.get("tool_mode") or "mock",
            }
        except Exception:
            _cfg = {"overrides": {}, "mode": "mock"}
        _cfg_at = time.time()
        return _cfg


def describe(name: str) -> dict[str, Any]:
    t = TOOLS[name]
    cfg = _config()
    ov = (cfg["overrides"] or {}).get(name, {})
    mode = ov.get("mode", cfg["mode"])
    return {
        "name": name,
        "mode": mode if (mode == "mock" or t.live) else "mock",
        "has_live": t.live is not None,
        "latency_ms": ov.get("latency_ms", t.latency_ms),
        "failure_rate": ov.get("failure_rate", t.failure_rate),
        "side_effecting": t.side_effecting,
    }


def call(name: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Invoke a tool.

    A tool failure is INFRA, never CAPABILITY: the upstream broke, which says
    nothing about whether the model was capable. Escalating here would spend
    senior tokens on a network blip.
    """
    if name not in TOOLS:
        raise PoisonFailure(f"unknown tool {name!r}")

    t = TOOLS[name]
    d = describe(name)
    payload = payload or {}

    if d["mode"] == "mock":
        time.sleep(d["latency_ms"] / 1000.0)

    if random.random() < float(d["failure_rate"]):
        raise InfraFailure(f"{name} unavailable (injected)")

    impl = t.live if (d["mode"] == "live" and t.live) else t.mock
    started = time.time()
    try:
        out = impl(payload)
    except (InfraFailure, PoisonFailure):
        raise
    except Exception as exc:
        raise InfraFailure(f"{name}: {type(exc).__name__}: {exc}") from exc

    return {"tool": name, "mode": d["mode"],
            "ms": round((time.time() - started) * 1000), **out}


# =============================================================================
# MOCK implementations -- fast, deterministic, safe on stage
# =============================================================================

@tool("logs", latency_ms=800, failure_rate=0.05)
def _logs(p: dict) -> dict:
    return {"matches": 42, "top_error": "UpstreamTimeout on charge()"}


@tool("github", latency_ms=300, failure_rate=0.02)
def _github(p: dict) -> dict:
    return {"recent_deploys": 2, "suspect_pr": "#8814"}


@tool("jira", latency_ms=500, failure_rate=0.10, side_effecting=True)
def _jira(p: dict) -> dict:
    return {"ticket": "PAY-4471", "action": p.get("action", "read")}


@tool("http", latency_ms=250, failure_rate=0.03)
def _http(p: dict) -> dict:
    return {"status": 200, "bytes": 1234, "url": p.get("url", "https://example.com")}


@tool("files", latency_ms=120, failure_rate=0.01)
def _files(p: dict) -> dict:
    return {"files_scanned": 19, "hits": 3, "pattern": p.get("pattern", "TODO")}


@tool("shell", latency_ms=150, failure_rate=0.02)
def _shell(p: dict) -> dict:
    return {"exit_code": 0, "stdout": "(mocked)", "cmd": p.get("cmd", "true")}


@tool("metrics_db", latency_ms=400, failure_rate=0.04)
def _metrics_db(p: dict) -> dict:
    return {"p99_ms": 4700, "error_rate": 0.061, "window": p.get("window", "1h")}


@tool("slack", latency_ms=250, failure_rate=0.05, side_effecting=True)
def _slack(p: dict) -> dict:
    return {"posted": True, "channel": p.get("channel", "#oncall")}


@tool("pagerduty", latency_ms=350, failure_rate=0.06, side_effecting=True)
def _pagerduty(p: dict) -> dict:
    return {"incident": "PD-9921", "severity": p.get("severity", "sev2")}


# =============================================================================
# LIVE implementations -- these genuinely do the work
# =============================================================================

@live_impl("files")
def _files_live(p: dict) -> dict:
    """Really greps this repository. Real I/O, no network, always available."""
    pattern = p.get("pattern", "TODO")
    root = Path(p.get("root", REPO_ROOT))
    hits, scanned = [], 0
    for f in root.rglob("*.py"):
        if ".venv" in f.parts or "__pycache__" in f.parts:
            continue
        scanned += 1
        try:
            for i, line in enumerate(f.read_text(errors="ignore").splitlines(), 1):
                if pattern in line:
                    hits.append({"file": str(f.relative_to(root)), "line": i})
        except OSError:
            continue
    return {"files_scanned": scanned, "hits": len(hits), "sample": hits[:5]}


@live_impl("shell")
def _shell_live(p: dict) -> dict:
    """
    Really runs a command, from an allowlist.

    Allowlisted rather than arbitrary: a task definition is data that an agent
    plan can point at, so an unrestricted shell would let any submitted plan run
    anything on the worker.
    """
    allowed = {"git-log": ["git", "log", "--oneline", "-10"],
               "git-status": ["git", "status", "--short"],
               "pytest-collect": ["python", "-m", "pytest", "--collect-only", "-q"],
               "disk": ["df", "-h", "."]}
    key = p.get("cmd", "git-log")
    if key not in allowed:
        raise PoisonFailure(f"command {key!r} not allowlisted")
    r = subprocess.run(allowed[key], cwd=REPO_ROOT, capture_output=True,
                       text=True, timeout=20)
    return {"exit_code": r.returncode,
            "stdout": r.stdout.strip()[:2000],
            "cmd": key}


@live_impl("http")
def _http_live(p: dict) -> dict:
    """Really fetches a URL over the network."""
    url = p.get("url", "https://api.github.com/zen")
    req = urllib.request.Request(url, headers={"User-Agent": "reliable-ai-agents"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read(4096)
            return {"status": resp.status, "bytes": len(body),
                    "url": url, "preview": body[:200].decode(errors="ignore")}
    except urllib.error.HTTPError as e:
        if 400 <= e.code < 500:
            raise PoisonFailure(f"{url} -> {e.code}")   # no retry will fix a 4xx
        raise InfraFailure(f"{url} -> {e.code}")


@live_impl("github")
def _github_live(p: dict) -> dict:
    """Real GitHub API. Public endpoints need no credentials."""
    repo = p.get("repo", "python/cpython")
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/commits?per_page=5",
        headers={"User-Agent": "reliable-ai-agents",
                 "Accept": "application/vnd.github+json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise (PoisonFailure if 400 <= e.code < 500 else InfraFailure)(
            f"github {repo} -> {e.code}"
        )
    return {"repo": repo, "recent_commits": len(data),
            "latest": (data[0]["commit"]["message"][:80] if data else None)}


@live_impl("metrics_db")
def _metrics_db_live(p: dict) -> dict:
    """Real query against our own Postgres -- genuine database work."""
    with pool().connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT count(*) AS attempts,
                      coalesce(sum(cost_units),0) AS cost,
                      count(*) FILTER (WHERE outcome='failed') AS failures
               FROM attempts"""
        )
        r = cur.fetchone()
    total = r["attempts"] or 1
    return {"attempts": r["attempts"], "cost_units": r["cost"],
            "error_rate": round(r["failures"] / total, 4)}


def set_mode(name: str | None, mode: str) -> None:
    """Switch one tool (or all, when name is None) between 'mock' and 'live'."""
    with pool().connection() as conn, conn.cursor() as cur:
        if name is None:
            cur.execute(
                "INSERT INTO runtime_config (key, value) VALUES ('tool_mode', %s) "
                "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                (json.dumps(mode),),
            )
        else:
            cur.execute(
                """UPDATE runtime_config
                   SET value = jsonb_set(value, %s, %s::jsonb, true)
                   WHERE key = 'tool_overrides'""",
                ([name], json.dumps({"mode": mode})),
            )
