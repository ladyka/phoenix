from __future__ import annotations

import logging
from asyncio import create_task, gather, sleep
from datetime import datetime, timedelta, timezone
from time import time
from typing import Iterable, Union

import sqlalchemy as sa
from sqlalchemy import exists, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql.roles import InElementRole

from phoenix.config import (
    DEFAULT_PROJECT_NAME,
    PLAYGROUND_PROJECT_NAME,
    get_env_delete_empty_projects,
)
from phoenix.db.constants import DEFAULT_PROJECT_TRACE_RETENTION_POLICY_ID
from phoenix.db.models import (
    DatasetEvaluators,
    Project,
    ProjectSession,
    ProjectTraceRetentionPolicy,
    Trace,
)
from phoenix.db.types.trace_retention import (
    MaxDaysOrCountRule,
    MaxDaysRule,
    TraceRetentionRule,
)
from phoenix.server.dml_event import ProjectDeleteEvent, SpanDeleteEvent
from phoenix.server.dml_event_handler import DmlEventHandler
from phoenix.server.prometheus import (
    RETENTION_POLICY_EXECUTIONS,
    RETENTION_PROJECTS_DELETED,
    RETENTION_SWEEPER_LAST_RUN,
)
from phoenix.server.types import DaemonTask, DbSessionFactory
from phoenix.utilities import hour_of_week

logger = logging.getLogger(__name__)

_PROTECTED_PROJECT_NAMES = frozenset({DEFAULT_PROJECT_NAME, PLAYGROUND_PROJECT_NAME})
_PROJECT_SESSION_DELETE_CHUNK_SIZE = 10_000


class TraceDataSweeper(DaemonTask):
    def __init__(self, db: DbSessionFactory, dml_event_handler: DmlEventHandler):
        super().__init__()
        self._db = db
        self._dml_event_handler = dml_event_handler

    async def _run(self) -> None:
        """Check hourly and apply policies."""
        while self._running:
            await self._sleep_until_next_hour()
            RETENTION_SWEEPER_LAST_RUN.set(time())
            try:
                if not (policies := await self._get_policies()):
                    continue
                current_hour = self._current_hour()
                if tasks := [
                    create_task(self._apply(policy))
                    for policy in policies
                    if self._should_apply(policy, current_hour)
                ]:
                    await gather(*tasks, return_exceptions=True)
            except Exception:
                logger.exception("Unexpected error in retention sweeper main loop")

    async def _get_policies(self) -> list[ProjectTraceRetentionPolicy]:
        stmt = sa.select(ProjectTraceRetentionPolicy).options(
            selectinload(ProjectTraceRetentionPolicy.projects).load_only(Project.id)
        )
        async with self._db() as session:
            result = await session.scalars(stmt)
        # filter out no-op policies, e.g. max_days == 0
        return [policy for policy in result if bool(policy.rule)]

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    def _current_hour(self) -> int:
        return hour_of_week(self._now())

    def _should_apply(self, policy: ProjectTraceRetentionPolicy, current_hour: int) -> bool:
        if current_hour != policy.cron_expression.get_hour_of_prev_run():
            return False
        if policy.id != DEFAULT_PROJECT_TRACE_RETENTION_POLICY_ID and not policy.projects:
            return False
        return True

    async def _apply(self, policy: ProjectTraceRetentionPolicy) -> None:
        try:
            project_rowids = (
                (sa.select(Project.id).where(Project.trace_retention_policy_id.is_(None)))
                if policy.id == DEFAULT_PROJECT_TRACE_RETENTION_POLICY_ID
                else [p.id for p in policy.projects]
            )
            max_days = _policy_max_days(policy.rule)
            delete_empty_projects = get_env_delete_empty_projects()
            async with self._db() as session:
                last_activity_by_project = await _fetch_project_last_activity(
                    session, project_rowids
                )
                affected_project_ids = await policy.rule.delete_traces(session, project_rowids)
                await _delete_orphan_project_sessions(session, project_rowids)
                deleted_project_ids: list[int] = []
                if delete_empty_projects and max_days > 0:
                    deleted_project_ids = await _delete_empty_projects(
                        session,
                        project_rowids,
                        max_days=max_days,
                        last_activity_by_project=last_activity_by_project,
                    )
            self._dml_event_handler.put(SpanDeleteEvent(tuple(affected_project_ids)))
            if deleted_project_ids:
                self._dml_event_handler.put(ProjectDeleteEvent(tuple(deleted_project_ids)))
                RETENTION_PROJECTS_DELETED.inc(len(deleted_project_ids))
                logger.info(
                    "Retention policy '%s' (id=%s) deleted %s empty project(s)",
                    policy.name,
                    policy.id,
                    len(deleted_project_ids),
                )
            RETENTION_POLICY_EXECUTIONS.labels(status="success").inc()
        except Exception:
            logger.exception(f"Failed to apply retention policy '{policy.name}' (id={policy.id})")
            RETENTION_POLICY_EXECUTIONS.labels(status="error").inc()

    async def _sleep_until_next_hour(self) -> None:
        next_hour = self._now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        await sleep((next_hour - self._now()).total_seconds())


def _policy_max_days(rule: TraceRetentionRule) -> float:
    root = rule.root
    if isinstance(root, (MaxDaysRule, MaxDaysOrCountRule)):
        return root.max_days
    return 0.0


async def _fetch_project_last_activity(
    session: AsyncSession,
    project_rowids: Union[Iterable[int], InElementRole],
) -> dict[int, datetime]:
    stmt = (
        sa.select(
            Project.id,
            func.coalesce(func.max(Trace.start_time), Project.created_at).label("last_activity_at"),
        )
        .outerjoin(Trace, Trace.project_rowid == Project.id)
        .where(Project.id.in_(project_rowids))
        .group_by(Project.id, Project.created_at)
    )
    rows = await session.execute(stmt)
    return {row.id: row.last_activity_at for row in rows}


async def _delete_orphan_project_sessions(
    session: AsyncSession,
    project_rowids: Union[Iterable[int], InElementRole],
) -> None:
    stmt = (
        sa.delete(ProjectSession)
        .where(ProjectSession.project_id.in_(project_rowids))
        .where(
            ~exists().where(
                Trace.project_session_rowid == ProjectSession.id,
            )
        )
    )
    await session.execute(stmt)


async def _delete_empty_projects(
    session: AsyncSession,
    project_rowids: Union[Iterable[int], InElementRole],
    *,
    max_days: float,
    last_activity_by_project: dict[int, datetime],
) -> list[int]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_days)
    stale_project_ids = [
        project_id
        for project_id, last_activity_at in last_activity_by_project.items()
        if last_activity_at < cutoff
    ]
    if not stale_project_ids:
        return []

    deleted_project_ids: list[int] = []
    delete_stmt = (
        sa.delete(Project)
        .where(Project.name.not_in(_PROTECTED_PROJECT_NAMES))
        .where(~exists().where(Trace.project_rowid == Project.id))
        .where(~exists().where(DatasetEvaluators.project_id == Project.id))
        .returning(Project.id)
    )
    for i in range(0, len(stale_project_ids), _PROJECT_SESSION_DELETE_CHUNK_SIZE):
        chunk = stale_project_ids[i : i + _PROJECT_SESSION_DELETE_CHUNK_SIZE]
        deleted_project_ids.extend(
            (await session.scalars(delete_stmt.where(Project.id.in_(chunk)))).all()
        )
    return deleted_project_ids
