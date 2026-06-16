"""
状态持久化模块
使用 SQLite 存储流程定义、实例、Token、任务、定时器等状态

持久化设计:
- 流程实例的完整状态(包括所有 Token、上下文)定期或状态变更时序列化入库
- 服务重启后, 通过 resume_all() 从数据库加载 RUNNING 状态的实例并恢复执行
- 人工任务和定时事件也持久化, 确保挂起状态不丢失
"""

from __future__ import annotations

import json
import sqlite3
import os
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from .models import (
    ProcessDefinition, ProcessInstance, Token, TaskInstance, TimerInstance,
    ProcessStatus, TokenStatus, TaskStatus, TimerStatus,
    Node, SequenceFlow, NodeType,
)


class PersistenceManager:
    """
    SQLite 持久化管理器

    表结构:
    - process_definitions: 流程定义 (JSON 序列化)
    - process_instances: 流程实例 (状态 + 上下文 JSON)
    - tokens: Token 状态 (每个 Token 单独存储)
    - task_instances: 人工任务
    - timer_instances: 定时事件
    """

    def __init__(self, db_path: str = "workflow.db"):
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def _get_conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._get_conn() as conn:
            c = conn.cursor()
            c.execute("""
                CREATE TABLE IF NOT EXISTS process_definitions (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    definition_json TEXT NOT NULL,
                    created_at REAL DEFAULT (strftime('%s','now'))
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS process_instances (
                    id TEXT PRIMARY KEY,
                    definition_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    context_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    completed_at REAL
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS tokens (
                    id TEXT PRIMARY KEY,
                    instance_id TEXT NOT NULL,
                    current_node_id TEXT,
                    status TEXT NOT NULL,
                    context_json TEXT NOT NULL,
                    parent_token_id TEXT,
                    fork_id TEXT,
                    FOREIGN KEY (instance_id) REFERENCES process_instances(id)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS task_instances (
                    id TEXT PRIMARY KEY,
                    instance_id TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    task_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    assignee TEXT,
                    form_data_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    completed_at REAL
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS timer_instances (
                    id TEXT PRIMARY KEY,
                    instance_id TEXT NOT NULL,
                    token_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    fire_time REAL NOT NULL,
                    status TEXT NOT NULL,
                    duration_seconds REAL,
                    cron_expression TEXT,
                    created_at REAL NOT NULL
                )
            """)

    def save_definition(self, definition: ProcessDefinition) -> None:
        def_data = {
            "id": definition.id,
            "name": definition.name,
            "nodes": [
                {"id": n.id, "name": n.name, "type": n.node_type.value, **n.properties}
                for n in definition.nodes.values()
            ],
            "flows": [
                {
                    "id": f.id, "source": f.source_id, "target": f.target_id,
                    "condition": f.condition, "name": f.name,
                }
                for f in definition.flows
            ],
        }
        with self._get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO process_definitions (id, name, definition_json) VALUES (?, ?, ?)",
                (definition.id, definition.name, json.dumps(def_data, ensure_ascii=False)),
            )

    def load_definition(self, definition_id: str) -> Optional[ProcessDefinition]:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT definition_json FROM process_definitions WHERE id = ?",
                (definition_id,),
            ).fetchone()
        if not row:
            return None
        from .parser import ProcessParser
        return ProcessParser.from_dict(json.loads(row["definition_json"]))

    def save_instance(self, instance: ProcessInstance) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO process_instances
                   (id, definition_id, status, context_json, created_at, updated_at, completed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    instance.id,
                    instance.definition_id,
                    instance.status.value,
                    json.dumps(instance.context, ensure_ascii=False),
                    instance.created_at,
                    instance.updated_at,
                    instance.completed_at,
                ),
            )
            conn.execute("DELETE FROM tokens WHERE instance_id = ?", (instance.id,))
            for token in instance.tokens.values():
                conn.execute(
                    """INSERT INTO tokens
                       (id, instance_id, current_node_id, status, context_json, parent_token_id, fork_id)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        token.id,
                        token.instance_id,
                        token.current_node_id,
                        token.status.value,
                        json.dumps(token.context, ensure_ascii=False),
                        token.parent_token_id,
                        token.fork_id,
                    ),
                )

    def load_instance(self, instance_id: str) -> Optional[ProcessInstance]:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM process_instances WHERE id = ?",
                (instance_id,),
            ).fetchone()
            if not row:
                return None
            token_rows = conn.execute(
                "SELECT * FROM tokens WHERE instance_id = ?",
                (instance_id,),
            ).fetchall()

        instance = ProcessInstance(
            id=row["id"],
            definition_id=row["definition_id"],
            status=ProcessStatus(row["status"]),
            context=json.loads(row["context_json"] or "{}"),
            tokens={},
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
        )
        for tr in token_rows:
            instance.tokens[tr["id"]] = Token(
                id=tr["id"],
                instance_id=tr["instance_id"],
                current_node_id=tr["current_node_id"],
                status=TokenStatus(tr["status"]),
                context=json.loads(tr["context_json"] or "{}"),
                parent_token_id=tr["parent_token_id"],
                fork_id=tr["fork_id"],
            )
        return instance

    def load_running_instances(self) -> List[ProcessInstance]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT id FROM process_instances WHERE status = ?",
                (ProcessStatus.RUNNING.value,),
            ).fetchall()
        return [self.load_instance(r["id"]) for r in rows if self.load_instance(r["id"])]

    def save_task(self, task: TaskInstance) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO task_instances
                   (id, instance_id, token_id, node_id, task_name, status,
                    assignee, form_data_json, created_at, completed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    task.id, task.instance_id, task.token_id, task.node_id,
                    task.task_name, task.status.value, task.assignee,
                    json.dumps(task.form_data, ensure_ascii=False),
                    task.created_at, task.completed_at,
                ),
            )

    def load_task(self, task_id: str) -> Optional[TaskInstance]:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM task_instances WHERE id = ?", (task_id,),
            ).fetchone()
        if not row:
            return None
        return TaskInstance(
            id=row["id"],
            instance_id=row["instance_id"],
            token_id=row["token_id"],
            node_id=row["node_id"],
            task_name=row["task_name"],
            status=TaskStatus(row["status"]),
            assignee=row["assignee"],
            form_data=json.loads(row["form_data_json"] or "{}"),
            created_at=row["created_at"],
            completed_at=row["completed_at"],
        )

    def load_tasks_by_instance(self, instance_id: str) -> List[TaskInstance]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT id FROM task_instances WHERE instance_id = ? ORDER BY created_at",
                (instance_id,),
            ).fetchall()
        return [t for t in (self.load_task(r["id"]) for r in rows) if t]

    def load_pending_tasks(self, assignee: Optional[str] = None) -> List[TaskInstance]:
        with self._get_conn() as conn:
            if assignee:
                rows = conn.execute(
                    """SELECT id FROM task_instances
                       WHERE status IN (?, ?) AND assignee = ? ORDER BY created_at""",
                    (TaskStatus.PENDING.value, TaskStatus.ASSIGNED.value, assignee),
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT id FROM task_instances
                       WHERE status IN (?, ?) ORDER BY created_at""",
                    (TaskStatus.PENDING.value, TaskStatus.ASSIGNED.value),
                ).fetchall()
        return [t for t in (self.load_task(r["id"]) for r in rows) if t]

    def save_timer(self, timer: TimerInstance) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO timer_instances
                   (id, instance_id, token_id, node_id, fire_time, status,
                    duration_seconds, cron_expression, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    timer.id, timer.instance_id, timer.token_id, timer.node_id,
                    timer.fire_time, timer.status.value, timer.duration_seconds,
                    timer.cron_expression, timer.created_at,
                ),
            )

    def load_timer(self, timer_id: str) -> Optional[TimerInstance]:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM timer_instances WHERE id = ?", (timer_id,),
            ).fetchone()
        if not row:
            return None
        return TimerInstance(
            id=row["id"],
            instance_id=row["instance_id"],
            token_id=row["token_id"],
            node_id=row["node_id"],
            fire_time=row["fire_time"],
            status=TimerStatus(row["status"]),
            duration_seconds=row["duration_seconds"],
            cron_expression=row["cron_expression"],
            created_at=row["created_at"],
        )

    def load_pending_timers(self) -> List[TimerInstance]:
        with self._get_conn() as conn:
            rows = conn.execute(
                """SELECT id FROM timer_instances
                   WHERE status = ? ORDER BY fire_time""",
                (TimerStatus.PENDING.value,),
            ).fetchall()
        return [t for t in (self.load_timer(r["id"]) for r in rows) if t]

    def delete_db(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
