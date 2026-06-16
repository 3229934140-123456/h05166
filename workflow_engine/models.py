"""
核心模型定义
包含流程图节点、流转、Token、流程实例等数据结构

核心设计思想:
- 流程 = 有向图: Node 是顶点, SequenceFlow 是边
- Token 是执行单元: 每个 Token 代表一条执行路径, 携带上下文数据
- 并行网关: 一个 Token 分叉成多个 Token 并行执行, 汇聚时等待所有分支到达
- 排他网关: 按条件选择一条出边, Token 继续单路径执行
"""

from __future__ import annotations

import uuid
import time
from enum import Enum
from typing import Any, Optional, Dict, List
from dataclasses import dataclass, field


class NodeType(str, Enum):
    START_EVENT = "startEvent"
    END_EVENT = "endEvent"
    TASK = "task"
    USER_TASK = "userTask"
    SERVICE_TASK = "serviceTask"
    EXCLUSIVE_GATEWAY = "exclusiveGateway"
    PARALLEL_GATEWAY = "parallelGateway"
    TIMER_EVENT = "timerEvent"
    INTERMEDIATE_EVENT = "intermediateEvent"


class GatewayType(str, Enum):
    EXCLUSIVE = "exclusive"
    PARALLEL = "parallel"


class EventType(str, Enum):
    START = "start"
    END = "end"
    TIMER = "timer"
    MESSAGE = "message"


class TokenStatus(str, Enum):
    ACTIVE = "active"
    WAITING = "waiting"
    SUSPENDED = "suspended"
    COMPLETED = "completed"
    CONSUMED = "consumed"


class ProcessStatus(str, Enum):
    RUNNING = "running"
    SUSPENDED = "suspended"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskStatus(str, Enum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class TimerStatus(str, Enum):
    PENDING = "pending"
    FIRED = "fired"
    CANCELLED = "cancelled"


@dataclass
class Node:
    id: str
    name: str
    node_type: NodeType
    properties: Dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        return self.properties.get(key, default)


@dataclass
class SequenceFlow:
    id: str
    source_id: str
    target_id: str
    condition: Optional[str] = None
    name: Optional[str] = None


@dataclass
class ProcessDefinition:
    """
    流程定义 - 有向图结构

    nodes: 顶点集合 (任务/网关/事件)
    flows: 边集合 (流转关系)
    """
    id: str
    name: str
    nodes: Dict[str, Node] = field(default_factory=dict)
    flows: List[SequenceFlow] = field(default_factory=list)

    def get_node(self, node_id: str) -> Optional[Node]:
        return self.nodes.get(node_id)

    def get_outgoing_flows(self, node_id: str) -> List[SequenceFlow]:
        return [f for f in self.flows if f.source_id == node_id]

    def get_incoming_flows(self, node_id: str) -> List[SequenceFlow]:
        return [f for f in self.flows if f.target_id == node_id]

    def get_start_node(self) -> Optional[Node]:
        for node in self.nodes.values():
            if node.node_type == NodeType.START_EVENT:
                return node
        return None


@dataclass
class Token:
    """
    Token - 流程执行单元

    每个 Token 代表流程中的一条执行路径:
    - 在单路径流程中只有 1 个 Token
    - 经过并行网关时 fork 出多个 Token 并行执行
    - 并行网关汇聚时 join, 所有分支 Token 到达后合成一个继续前进
    """
    id: str
    instance_id: str
    current_node_id: Optional[str]
    status: TokenStatus = TokenStatus.ACTIVE
    context: Dict[str, Any] = field(default_factory=dict)
    parent_token_id: Optional[str] = None
    fork_id: Optional[str] = None

    @classmethod
    def create(cls, instance_id: str, start_node_id: str,
               context: Optional[Dict[str, Any]] = None,
               parent_token_id: Optional[str] = None,
               fork_id: Optional[str] = None) -> "Token":
        return cls(
            id=str(uuid.uuid4()),
            instance_id=instance_id,
            current_node_id=start_node_id,
            status=TokenStatus.ACTIVE,
            context=context or {},
            parent_token_id=parent_token_id,
            fork_id=fork_id,
        )


@dataclass
class ProcessInstance:
    """
    流程实例

    持久化的核心对象, 包含:
    - 流程定义 ID
    - 全局上下文 (变量、业务数据)
    - 所有 Token 的状态
    """
    id: str
    definition_id: str
    status: ProcessStatus = ProcessStatus.RUNNING
    context: Dict[str, Any] = field(default_factory=dict)
    tokens: Dict[str, Token] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None

    @classmethod
    def create(cls, definition_id: str,
               context: Optional[Dict[str, Any]] = None) -> "ProcessInstance":
        return cls(
            id=str(uuid.uuid4()),
            definition_id=definition_id,
            status=ProcessStatus.RUNNING,
            context=context or {},
            tokens={},
        )

    def get_active_tokens(self) -> List[Token]:
        return [t for t in self.tokens.values()
                if t.status in (TokenStatus.ACTIVE, TokenStatus.WAITING, TokenStatus.SUSPENDED)]

    def get_tokens_at_node(self, node_id: str) -> List[Token]:
        return [t for t in self.tokens.values() if t.current_node_id == node_id]


@dataclass
class TaskInstance:
    """
    人工任务实例

    当 Token 到达 UserTask 时创建, 流程实例挂起等待:
    - Token 状态变为 SUSPENDED
    - Task 状态为 PENDING/ASSIGNED
    - 外部完成任务后调用 complete(), 恢复 Token 流转
    """
    id: str
    instance_id: str
    token_id: str
    node_id: str
    task_name: str
    status: TaskStatus = TaskStatus.PENDING
    assignee: Optional[str] = None
    form_data: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None

    @classmethod
    def create(cls, instance_id: str, token_id: str, node_id: str,
               task_name: str) -> "TaskInstance":
        return cls(
            id=str(uuid.uuid4()),
            instance_id=instance_id,
            token_id=token_id,
            node_id=node_id,
            task_name=task_name,
            status=TaskStatus.PENDING,
        )


@dataclass
class TimerInstance:
    """
    定时事件实例

    Token 到达 TimerEvent 时:
    - Token 状态变为 WAITING
    - 创建 TimerInstance 记录触发时间
    - 调度器到期后触发, 恢复 Token 流转
    """
    id: str
    instance_id: str
    token_id: str
    node_id: str
    fire_time: float
    status: TimerStatus = TimerStatus.PENDING
    duration_seconds: Optional[float] = None
    cron_expression: Optional[str] = None
    created_at: float = field(default_factory=time.time)

    @classmethod
    def create(cls, instance_id: str, token_id: str, node_id: str,
               fire_time: float, duration_seconds: Optional[float] = None,
               cron_expression: Optional[str] = None) -> "TimerInstance":
        return cls(
            id=str(uuid.uuid4()),
            instance_id=instance_id,
            token_id=token_id,
            node_id=node_id,
            fire_time=fire_time,
            status=TimerStatus.PENDING,
            duration_seconds=duration_seconds,
            cron_expression=cron_expression,
        )
