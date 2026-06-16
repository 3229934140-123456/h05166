"""
事件调度模块
负责定时事件的注册、扫描、触发

定时事件触发机制:
1. Token 到达 TimerEvent 节点时:
   - Token 状态变更为 WAITING
   - 创建 TimerInstance 并注册到调度器 (记录 fire_time)
2. 调度器线程 (或手动 tick) 定期扫描:
   - 找出所有 PENDING 且 fire_time <= now 的 TimerInstance
   - 标记 Timer 为 FIRED
   - 通知引擎恢复对应 Token 执行
3. 持久化保证: TimerInstance 持久化到 DB, 服务重启后从 DB 重新加载待触发的定时器
"""

from __future__ import annotations

import time
import threading
from typing import Optional, List, Callable, Dict, TYPE_CHECKING
from .models import TimerInstance, TimerStatus

if TYPE_CHECKING:
    from .persistence import PersistenceManager
    from .engine import WorkflowEngine


class EventScheduler:
    """
    事件调度器

    两种运行模式:
    - 自动模式: 启动后台线程按 polling_interval 秒轮询
    - 手动模式: 外部调用 tick() 手动触发扫描 (便于测试)
    """

    def __init__(self,
                 persistence: Optional["PersistenceManager"] = None,
                 polling_interval: float = 1.0):
        self.persistence = persistence
        self.polling_interval = polling_interval
        self._timers: Dict[str, TimerInstance] = {}
        self._engine: Optional["WorkflowEngine"] = None
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def set_engine(self, engine: "WorkflowEngine") -> None:
        self._engine = engine

    def schedule_timer(self, timer: TimerInstance) -> TimerInstance:
        with self._lock:
            self._timers[timer.id] = timer
            if self.persistence:
                self.persistence.save_timer(timer)
        return timer

    def cancel_timer(self, timer_id: str) -> None:
        with self._lock:
            timer = self._timers.get(timer_id)
            if timer:
                timer.status = TimerStatus.CANCELLED
                if self.persistence:
                    self.persistence.save_timer(timer)

    def fire_timer(self, timer_id: str) -> TimerInstance:
        """
        触发定时器并恢复对应 Token

        由调度器在到期时自动调用, 也可手动调用以提前触发
        幂等操作: 若定时器已经 FIRED, 直接返回不报错
        """
        with self._lock:
            timer = self._timers.get(timer_id)
            if not timer and self.persistence:
                timer = self.persistence.load_timer(timer_id)
            if not timer:
                raise ValueError(f"定时器不存在: {timer_id}")
            if timer.status == TimerStatus.FIRED:
                return timer
            if timer.status != TimerStatus.PENDING:
                raise RuntimeError(f"定时器状态不正确: {timer.status}")
            timer.status = TimerStatus.FIRED
            if self.persistence:
                self.persistence.save_timer(timer)
            if timer.id in self._timers:
                del self._timers[timer.id]
            return timer

    def tick(self) -> List[TimerInstance]:
        """
        手动触发一次扫描, 返回本次触发的所有定时器

        这是调度的核心:
        - 首先从数据库同步最新的 Timer 状态 (保证持久化数据优先)
        - 扫描所有 PENDING 状态且 fire_time <= now 的定时器
        - 触发后通知引擎恢复 Token 流转
        """
        fired = []
        all_timers = []
        with self._lock:
            now = time.time()

            if self.persistence:
                db_pending = self.persistence.load_pending_timers()
                for t in db_pending:
                    self._timers[t.id] = t
            all_timers = list(self._timers.values())

            to_fire = [
                t for t in all_timers
                if t.status == TimerStatus.PENDING and t.fire_time <= now
            ]

        for timer in to_fire:
            try:
                if self._engine:
                    self._engine.trigger_timer(timer.id)
                fired.append(self._timers.get(timer.id) or timer)
            except Exception as e:
                print(f"触发定时器 {timer.id} 失败: {e}")

        return fired

    def start(self) -> None:
        """启动后台调度线程"""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """停止后台调度线程"""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.tick()
            except Exception as e:
                print(f"调度器扫描出错: {e}")
            self._stop_event.wait(self.polling_interval)

    def get_pending_timers(self) -> List[TimerInstance]:
        with self._lock:
            if self.persistence:
                return self.persistence.load_pending_timers()
            return [t for t in self._timers.values() if t.status == TimerStatus.PENDING]
