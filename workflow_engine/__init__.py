"""
BPMN 风格工作流引擎
支持流程定义解析、Token 流转、并行/排他网关、人工任务、定时事件、状态持久化
"""

from .models import (
    NodeType,
    GatewayType,
    EventType,
    TokenStatus,
    ProcessStatus,
    TaskStatus,
    TimerStatus,
    Node,
    SequenceFlow,
    ProcessDefinition,
    Token,
    ProcessInstance,
    TaskInstance,
    TimerInstance,
)
from .parser import ProcessParser
from .engine import WorkflowEngine
from .persistence import PersistenceManager
from .task_manager import TaskManager
from .scheduler import EventScheduler

__version__ = "1.0.0"
__all__ = [
    "NodeType",
    "GatewayType",
    "EventType",
    "TokenStatus",
    "ProcessStatus",
    "TaskStatus",
    "TimerStatus",
    "Node",
    "SequenceFlow",
    "ProcessDefinition",
    "Token",
    "ProcessInstance",
    "TaskInstance",
    "TimerInstance",
    "ProcessParser",
    "WorkflowEngine",
    "PersistenceManager",
    "TaskManager",
    "EventScheduler",
]
