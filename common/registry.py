"""
Pluggable task registry.

Adding a capability to the runtime is ONE decorated function. You do not edit a
central dict, and you do not touch the worker:

    # tasks/search_incidents.py
    from common.registry import task

    @task(8, name="search-incidents", tool="logs")
    def search_incidents(ctx):
        return {"hits": [...]}

`discover()` imports every module under tasks/, which fires the decorators. Drop
a file in, restart a worker, and the task is live.
"""

from __future__ import annotations

import importlib
import logging
import pkgutil
from typing import Any, Callable, Iterator

from common.protocol import TaskContext, TaskDef

log = logging.getLogger(__name__)


class Registry:
    """Task id -> TaskDef. Duplicate ids are a hard error, not a silent overwrite."""

    def __init__(self) -> None:
        self._defs: dict[int, TaskDef] = {}

    def add(self, task_def: TaskDef) -> TaskDef:
        existing = self._defs.get(task_def.id)
        if existing is not None and existing.name != task_def.name:
            raise ValueError(
                f"task id {task_def.id} already registered as {existing.name!r}; "
                f"refusing to overwrite with {task_def.name!r}"
            )
        self._defs[task_def.id] = task_def
        return task_def

    def task(
        self,
        id: int,
        name: str | None = None,
        *,
        difficulty: str = "easy",
        side_effecting: bool = False,
        tool: str | None = None,
    ) -> Callable[[Callable[[TaskContext], dict[str, Any]]], Callable]:
        """Decorator form. The function keeps working as a plain function."""

        def wrap(fn: Callable[[TaskContext], dict[str, Any]]):
            self.add(
                TaskDef(
                    id=id,
                    name=name or fn.__name__.replace("_", "-"),
                    run=fn,
                    difficulty=difficulty,      # type: ignore[arg-type]
                    side_effecting=side_effecting,
                    tool=tool,
                )
            )
            return fn

        return wrap

    def get(self, task_id: int) -> TaskDef | None:
        return self._defs.get(task_id)

    def as_dict(self) -> dict[int, TaskDef]:
        return dict(self._defs)

    def merge(self, other: dict[int, TaskDef]) -> None:
        for td in other.values():
            self.add(td)

    def __len__(self) -> int:
        return len(self._defs)

    def __contains__(self, task_id: object) -> bool:
        return task_id in self._defs

    def __iter__(self) -> Iterator[TaskDef]:
        return iter(self._defs.values())


# Module-level singleton, and the decorator most code will import.
registry = Registry()
task = registry.task


def discover(package: str = "tasks") -> Registry:
    """
    Import every module in `package` so decorators fire, then fold in a legacy
    TASK_DEFS dict if that package still exports one.

    Both registration styles work, so this never breaks a lane mid-hackathon.
    """
    try:
        pkg = importlib.import_module(package)
    except ModuleNotFoundError:
        log.warning("package %s not importable -- no tasks registered", package)
        return registry

    for mod in pkgutil.iter_modules(pkg.__path__):
        if mod.name.startswith("_"):
            continue
        try:
            importlib.import_module(f"{package}.{mod.name}")
        except Exception:
            log.exception("failed importing %s.%s", package, mod.name)

    legacy = getattr(pkg, "TASK_DEFS", None)
    if legacy is None:
        try:
            legacy = getattr(
                importlib.import_module(f"{package}.registry"), "TASK_DEFS", None
            )
        except ModuleNotFoundError:
            legacy = None
    if legacy:
        registry.merge(legacy)

    log.info("registry: %d task(s) available", len(registry))
    return registry
