"""
执行引擎核心
基于 Token 流转驱动流程执行

核心机制:
1. Token 驱动: 每个 Token 代表一条执行路径, 引擎驱动所有 ACTIVE 状态的 Token 前进
2. 排他网关: 遍历出边, 按条件表达式求值, 选择第一条满足条件的边, Token 继续单路径
3. 并行网关(分叉): Token 到达后, 为每条出边 fork 一个新 Token, 原 Token 标记 CONSUMED
4. 并行网关(汇聚): 检查该网关的所有入边是否都有 Token 到达(同一 fork_id), 全部到达后合成一个 Token 继续前进
5. 人工任务: Token 到达后状态变为 SUSPENDED, 创建 TaskInstance, 等待外部 complete()
6. 定时事件: Token 到达后状态变为 WAITING, 创建 TimerInstance, 等待调度器触发
"""

from __future__ import annotations

import uuid
import time
import re
from typing import Any, Dict, List, Optional, Callable, TYPE_CHECKING
from .models import (
    Node, NodeType, ProcessDefinition, ProcessInstance, Token,
    TokenStatus, ProcessStatus, TaskInstance, TimerInstance, TaskStatus,
)

if TYPE_CHECKING:
    from .persistence import PersistenceManager
    from .task_manager import TaskManager
    from .scheduler import EventScheduler


class ConditionEvaluator:
    """
    条件表达式求值器
    支持简单的 Python 表达式, 如: "amount > 1000", "status == 'approved'"
    上下文中的变量可直接引用
    """

    @staticmethod
    def evaluate(expression: str, context: Dict[str, Any]) -> bool:
        if not expression or not expression.strip():
            return True
        try:
            safe_dict = {k: v for k, v in context.items() if not k.startswith("_")}
            result = eval(expression, {"__builtins__": {}}, safe_dict)
            return bool(result)
        except Exception as e:
            raise ValueError(f"条件表达式求值失败: '{expression}', 错误: {e}")


class WorkflowEngine:
    """
    工作流执行引擎

    组件协作:
    - parser: 解析流程定义
    - persistence: 持久化流程实例状态
    - task_manager: 管理人工任务
    - scheduler: 调度定时事件
    """

    def __init__(self,
                 persistence: Optional["PersistenceManager"] = None,
                 task_manager: Optional["TaskManager"] = None,
                 scheduler: Optional["EventScheduler"] = None):
        self.definitions: Dict[str, ProcessDefinition] = {}
        self.persistence = persistence
        self.task_manager = task_manager
        self.scheduler = scheduler
        self.service_handlers: Dict[str, Callable] = {}
        self.condition_evaluator = ConditionEvaluator()

    def register_definition(self, definition: ProcessDefinition) -> None:
        self.definitions[definition.id] = definition
        if self.persistence:
            self.persistence.save_definition(definition)

    def register_service_handler(self, service_name: str,
                                 handler: Callable[[Dict[str, Any]], Dict[str, Any]]) -> None:
        self.service_handlers[service_name] = handler

    def start_process(self, definition_id: str,
                      context: Optional[Dict[str, Any]] = None) -> ProcessInstance:
        definition = self.definitions.get(definition_id)
        if not definition:
            raise ValueError(f"流程定义不存在: {definition_id}")

        start_node = definition.get_start_node()
        if not start_node:
            raise ValueError(f"流程定义没有开始节点: {definition_id}")

        instance = ProcessInstance.create(definition_id, context)
        token = Token.create(instance.id, start_node.id, dict(context or {}))
        instance.tokens[token.id] = token

        self._persist_instance(instance)

        self._continue_process(instance)
        return instance

    def complete_task(self, task_id: str, form_data: Optional[Dict[str, Any]] = None) -> None:
        if not self.task_manager:
            raise RuntimeError("未配置 TaskManager")

        task = self.task_manager.complete_task(task_id, form_data or {})
        instance = self._load_instance(task.instance_id)
        if not instance:
            raise ValueError(f"流程实例不存在: {task.instance_id}")

        token = instance.tokens.get(task.token_id)
        if not token:
            raise ValueError(f"Token 不存在: {task.token_id}")

        definition = self.definitions.get(instance.definition_id)
        if form_data:
            token.context.update(form_data)
            instance.context.update(form_data)

        self._advance_from_current(instance, token, definition)
        self._persist_instance(instance)
        self._continue_process(instance)

    def trigger_timer(self, timer_id: str) -> None:
        if not self.scheduler:
            raise RuntimeError("未配置 EventScheduler")

        timer = self.scheduler.fire_timer(timer_id)
        instance = self._load_instance(timer.instance_id)
        if not instance:
            raise ValueError(f"流程实例不存在: {timer.instance_id}")

        token = instance.tokens.get(timer.token_id)
        if not token:
            raise ValueError(f"Token 不存在: {timer.token_id}")

        definition = self.definitions.get(instance.definition_id)
        self._advance_from_current(instance, token, definition)
        self._persist_instance(instance)
        self._continue_process(instance)

    def get_instance(self, instance_id: str) -> Optional[ProcessInstance]:
        return self._load_instance(instance_id)

    def _continue_process(self, instance: ProcessInstance) -> None:
        """
        驱动流程实例继续执行
        循环处理所有 ACTIVE 的 Token, 直到没有可继续的 Token 或流程结束
        """
        definition = self.definitions.get(instance.definition_id)
        if not definition:
            raise ValueError(f"流程定义不存在: {instance.definition_id}")

        progressed = True
        while progressed:
            progressed = False
            active_tokens = [t for t in instance.tokens.values()
                             if t.status == TokenStatus.ACTIVE]

            for token in active_tokens:
                if token.status != TokenStatus.ACTIVE:
                    continue

                node = definition.get_node(token.current_node_id)
                if not node:
                    continue

                result = self._process_node(instance, token, node, definition)
                if result:
                    progressed = True

            if instance.status != ProcessStatus.RUNNING:
                break

        instance.updated_at = time.time()

        if not instance.get_active_tokens():
            instance.status = ProcessStatus.COMPLETED
            instance.completed_at = time.time()

        self._persist_instance(instance)

    def _process_node(self, instance: ProcessInstance, token: Token,
                      node: Node, definition: ProcessDefinition) -> bool:
        """
        处理单个节点, 返回是否推进了流程

        节点处理逻辑:
        - START_EVENT: 直接流转到下一个节点
        - END_EVENT: Token 标记为 COMPLETED
        - TASK/SERVICE_TASK: 执行后流转
        - USER_TASK: 挂起 Token, 创建人工任务
        - EXCLUSIVE_GATEWAY: 按条件选一条出边
        - PARALLEL_GATEWAY: 分叉(fork)或汇聚(join)
        - TIMER_EVENT: 挂起 Token, 创建定时事件
        """
        node_type = node.node_type

        if node_type == NodeType.START_EVENT:
            return self._advance_token(instance, token, definition)

        elif node_type == NodeType.END_EVENT:
            token.status = TokenStatus.COMPLETED
            return True

        elif node_type in (NodeType.TASK, NodeType.SERVICE_TASK):
            return self._execute_task(instance, token, node, definition)

        elif node_type == NodeType.USER_TASK:
            return self._handle_user_task(instance, token, node)

        elif node_type == NodeType.EXCLUSIVE_GATEWAY:
            return self._handle_exclusive_gateway(instance, token, node, definition)

        elif node_type == NodeType.PARALLEL_GATEWAY:
            return self._handle_parallel_gateway(instance, token, node, definition)

        elif node_type == NodeType.TIMER_EVENT:
            return self._handle_timer_event(instance, token, node)

        return False

    def _advance_from_current(self, instance: ProcessInstance, token: Token,
                              definition: ProcessDefinition) -> None:
        """
        从当前挂起节点(UserTask/TimerEvent)前进到下一个节点

        当人工任务完成或定时事件触发时调用:
        - Token 已经在当前节点, 不需要再处理该节点
        - 直接沿出边跳到下一个节点, 设为 ACTIVE 状态
        """
        flows = definition.get_outgoing_flows(token.current_node_id)
        token.status = TokenStatus.ACTIVE
        if flows and len(flows) == 1:
            target = definition.get_node(flows[0].target_id)
            if target:
                token.current_node_id = target.id

    def _advance_token(self, instance: ProcessInstance, token: Token,
                       definition: ProcessDefinition) -> bool:
        """Token 沿出边前进到下一个节点"""
        flows = definition.get_outgoing_flows(token.current_node_id)
        if not flows:
            token.status = TokenStatus.COMPLETED
            return True

        if len(flows) == 1:
            target = definition.get_node(flows[0].target_id)
            if target:
                token.current_node_id = target.id
                return True

        token.status = TokenStatus.COMPLETED
        return True

    def _execute_task(self, instance: ProcessInstance, token: Token,
                      node: Node, definition: ProcessDefinition) -> bool:
        """执行服务任务/自动任务"""
        service_name = node.get("service") or node.get("handler")
        if service_name and service_name in self.service_handlers:
            try:
                result = self.service_handlers[service_name](token.context)
                if isinstance(result, dict):
                    token.context.update(result)
                    instance.context.update(result)
            except Exception as e:
                print(f"服务任务执行失败 {node.name}: {e}")

        return self._advance_token(instance, token, definition)

    def _handle_user_task(self, instance: ProcessInstance, token: Token, node: Node) -> bool:
        """
        处理人工任务:
        - Token 状态变为 SUSPENDED
        - 创建 TaskInstance 记录
        - 流程实例挂起, 等待外部完成
        """
        token.status = TokenStatus.SUSPENDED

        if self.task_manager:
            task = TaskInstance.create(
                instance_id=instance.id,
                token_id=token.id,
                node_id=node.id,
                task_name=node.name,
            )
            assignee = node.get("assignee")
            if assignee:
                task.assignee = assignee
                task.status = TaskStatus.ASSIGNED
            self.task_manager.create_task(task)

        return True

    def _handle_exclusive_gateway(self, instance: ProcessInstance, token: Token,
                                  node: Node, definition: ProcessDefinition) -> bool:
        """
        排他网关(XOR): 按条件选择一条出边

        执行逻辑:
        1. 获取所有出边
        2. 按顺序求值每条边的 condition
        3. 选择第一条求值为 True 的边 (没有条件默认为 True)
        4. Token 沿该边继续前进
        5. 如果没有满足条件的边, 走 default 或报错
        """
        flows = definition.get_outgoing_flows(node.id)
        if not flows:
            token.status = TokenStatus.COMPLETED
            return True

        context = {**instance.context, **token.context}
        selected_flow = None
        default_flow_id = node.get("default")

        for flow in flows:
            if flow.condition:
                if self.condition_evaluator.evaluate(flow.condition, context):
                    selected_flow = flow
                    break
            else:
                if not default_flow_id or flow.id == default_flow_id:
                    selected_flow = flow

        if not selected_flow and default_flow_id:
            selected_flow = next((f for f in flows if f.id == default_flow_id), None)

        if not selected_flow:
            raise RuntimeError(
                f"排他网关 {node.name}({node.id}) 没有满足条件的出边"
            )

        target = definition.get_node(selected_flow.target_id)
        if target:
            token.current_node_id = target.id
        return True

    def _handle_parallel_gateway(self, instance: ProcessInstance, token: Token,
                                 node: Node, definition: ProcessDefinition) -> bool:
        """
        并行网关(AND): 分叉(Fork) 或 汇聚(Join)

        判断逻辑:
        - 如果 Token 到达时还有其他入边未到达 -> 这是汇聚点(Join): 等待所有分支
        - 如果所有入边都已到达 -> 汇聚完成, 合成一个 Token 继续
        - 如果只有一条入边活跃 -> 这是分叉点(Fork): 为每条出边创建新 Token

        并发汇合正确性保证:
        1. 每个 fork 操作生成唯一的 fork_id
        2. 同 fork_id 的所有分支 Token 在汇聚时互相识别
        3. 汇聚时计数该 fork_id 下到达该网关的 Token 数
        4. 数量等于该网关的入边数时才继续, 防止提前汇合
        5. 已汇合的 Token 标记 CONSUMED, 防止重复汇合
        """
        incoming_flows = definition.get_incoming_flows(node.id)
        outgoing_flows = definition.get_outgoing_flows(node.id)

        fork_id = token.fork_id or f"fork_{node.id}_{uuid.uuid4().hex[:8]}"

        incoming_count = len(incoming_flows)

        if incoming_count > 1:
            tokens_at_node = instance.get_tokens_at_node(node.id)
            arrived_tokens = [
                t for t in tokens_at_node
                if t.fork_id == fork_id
                and t.status in (TokenStatus.ACTIVE, TokenStatus.WAITING)
                and t.id != token.id
            ]
            arrived_count = len(arrived_tokens) + 1

            if arrived_count < incoming_count:
                token.status = TokenStatus.WAITING
                token.fork_id = fork_id
                return True

            for t in arrived_tokens:
                t.status = TokenStatus.CONSUMED
            token.status = TokenStatus.CONSUMED

            if outgoing_flows:
                merged_context = {}
                all_tokens = arrived_tokens + [token]
                for t in all_tokens:
                    merged_context.update(t.context)
                token.context = merged_context
                instance.context.update(merged_context)

                if len(outgoing_flows) == 1:
                    new_token = Token.create(
                        instance.id, outgoing_flows[0].target_id,
                        dict(merged_context),
                        parent_token_id=token.id,
                        fork_id=fork_id,
                    )
                    instance.tokens[new_token.id] = new_token
                else:
                    new_fork_id = f"fork_{node.id}_{uuid.uuid4().hex[:8]}"
                    for flow in outgoing_flows:
                        child = Token.create(
                            instance.id, flow.target_id,
                            dict(merged_context),
                            parent_token_id=token.id,
                            fork_id=new_fork_id,
                        )
                        instance.tokens[child.id] = child
            return True
        else:
            token.status = TokenStatus.CONSUMED

            if len(outgoing_flows) == 1:
                new_token = Token.create(
                    instance.id, outgoing_flows[0].target_id,
                    dict(token.context),
                    parent_token_id=token.id,
                    fork_id=fork_id,
                )
                instance.tokens[new_token.id] = new_token
            else:
                for flow in outgoing_flows:
                    child = Token.create(
                        instance.id, flow.target_id,
                        dict(token.context),
                        parent_token_id=token.id,
                        fork_id=fork_id,
                    )
                    instance.tokens[child.id] = child
            return True

    def _handle_timer_event(self, instance: ProcessInstance, token: Token, node: Node) -> bool:
        """
        处理定时事件:
        - Token 状态变为 WAITING
        - 创建 TimerInstance, 记录触发时间
        - 注册到调度器
        """
        token.status = TokenStatus.WAITING

        duration = node.get("duration")
        duration_seconds = None
        fire_time = None

        if duration:
            duration_seconds = self._parse_duration(duration)
            fire_time = time.time() + duration_seconds

        cron_expr = node.get("cron")
        if cron_expr:
            fire_time = fire_time or (time.time() + 60)

        if not fire_time:
            fire_time = time.time() + 60

        if self.scheduler:
            timer = TimerInstance.create(
                instance_id=instance.id,
                token_id=token.id,
                node_id=node.id,
                fire_time=fire_time,
                duration_seconds=duration_seconds,
                cron_expression=cron_expr,
            )
            self.scheduler.schedule_timer(timer)

        return True

    @staticmethod
    def _parse_duration(duration: str) -> float:
        """解析持续时间, 支持格式如: '30s', '5m', '2h', '1d'"""
        match = re.match(r"^(\d+(?:\.\d+)?)\s*([smhd]?)$", str(duration).strip().lower())
        if not match:
            try:
                return float(duration)
            except ValueError:
                raise ValueError(f"无效的持续时间格式: {duration}")

        value = float(match.group(1))
        unit = match.group(2) or "s"
        multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
        return value * multipliers[unit]

    def _persist_instance(self, instance: ProcessInstance) -> None:
        if self.persistence:
            self.persistence.save_instance(instance)

    def _load_instance(self, instance_id: str) -> Optional[ProcessInstance]:
        if self.persistence:
            return self.persistence.load_instance(instance_id)
        return None

    def resume_all(self) -> None:
        """服务重启后恢复所有持久化的流程实例"""
        if not self.persistence:
            return
        instances = self.persistence.load_running_instances()
        for instance in instances:
            if instance.status == ProcessStatus.RUNNING:
                self._continue_process(instance)
