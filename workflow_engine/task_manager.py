"""
任务管理模块
管理人工任务的创建、分配、完成、取消等生命周期

人工任务挂起/恢复机制:
1. 当 Token 到达 UserTask 节点时:
   - Token 状态变更为 SUSPENDED
   - 创建 TaskInstance (PENDING 状态)
   - 流程实例在此分支挂起, 其他并行分支继续执行
2. 外部系统/用户调用 complete_task():
   - TaskInstance 状态变更为 COMPLETED, 记录 form_data
   - 对应的 Token 状态恢复为 ACTIVE
   - 引擎继续驱动该 Token 沿流程前进
3. 持久化保证: TaskInstance 和 Token 状态都持久化到 DB, 服务重启后挂起状态不丢失
"""

from __future__ import annotations

import time
from typing import Optional, List, Dict, Any, TYPE_CHECKING
from .models import TaskInstance, TaskStatus

if TYPE_CHECKING:
    from .persistence import PersistenceManager


class TaskManager:
    """人工任务管理器"""

    def __init__(self, persistence: Optional["PersistenceManager"] = None):
        self.persistence = persistence

    def create_task(self, task: TaskInstance) -> TaskInstance:
        if self.persistence:
            self.persistence.save_task(task)
        return task

    def assign_task(self, task_id: str, assignee: str) -> TaskInstance:
        task = self._get_task(task_id)
        if task.status != TaskStatus.PENDING:
            raise RuntimeError(f"任务状态不允许分配: {task.status}")
        task.assignee = assignee
        task.status = TaskStatus.ASSIGNED
        if self.persistence:
            self.persistence.save_task(task)
        return task

    def complete_task(self, task_id: str, form_data: Optional[Dict[str, Any]] = None) -> TaskInstance:
        """
        完成人工任务

        这是恢复挂起流程的关键入口:
        - 标记任务完成, 保存表单数据
        - 引擎收到通知后恢复对应 Token 的 ACTIVE 状态并继续流转
        """
        task = self._get_task(task_id)
        if task.status in (TaskStatus.COMPLETED, TaskStatus.CANCELLED):
            raise RuntimeError(f"任务已结束, 无法重复完成: {task.status}")
        task.status = TaskStatus.COMPLETED
        task.completed_at = time.time()
        if form_data:
            task.form_data.update(form_data)
        if self.persistence:
            self.persistence.save_task(task)
        return task

    def cancel_task(self, task_id: str) -> TaskInstance:
        task = self._get_task(task_id)
        task.status = TaskStatus.CANCELLED
        task.completed_at = time.time()
        if self.persistence:
            self.persistence.save_task(task)
        return task

    def get_task(self, task_id: str) -> Optional[TaskInstance]:
        return self._get_task(task_id)

    def list_pending_tasks(self, assignee: Optional[str] = None) -> List[TaskInstance]:
        if self.persistence:
            return self.persistence.load_pending_tasks(assignee)
        return []

    def list_instance_tasks(self, instance_id: str) -> List[TaskInstance]:
        if self.persistence:
            return self.persistence.load_tasks_by_instance(instance_id)
        return []

    def _get_task(self, task_id: str) -> TaskInstance:
        task = None
        if self.persistence:
            task = self.persistence.load_task(task_id)
        if not task:
            raise ValueError(f"任务不存在: {task_id}")
        return task
