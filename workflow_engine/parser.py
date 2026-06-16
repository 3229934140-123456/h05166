"""
流程定义解析模块
支持从 JSON 格式解析 BPMN 风格的流程定义

流程 = 有向图建模:
    节点(Node) = 顶点: 开始/结束事件、任务、排他网关、并行网关、定时事件
    流转(SequenceFlow) = 有向边: 连接节点, 可带条件表达式
"""

from __future__ import annotations

import json
from typing import Dict, Any, List, Optional
from .models import (
    Node, NodeType, SequenceFlow, ProcessDefinition,
)


class ProcessParser:
    """
    流程定义解析器

    支持的 JSON 格式示例:
    {
        "id": "process_001",
        "name": "请假审批流程",
        "nodes": [
            {"id": "start", "name": "开始", "type": "startEvent"},
            {"id": "apply", "name": "提交申请", "type": "userTask"},
            {"id": "gateway1", "name": "天数判断", "type": "exclusiveGateway"},
            {"id": "approve_leader", "name": "主管审批", "type": "userTask"},
            {"id": "approve_hr", "name": "HR审批", "type": "userTask"},
            {"id": "end", "name": "结束", "type": "endEvent"}
        ],
        "flows": [
            {"id": "f1", "source": "start", "target": "apply"},
            {"id": "f2", "source": "apply", "target": "gateway1"},
            {"id": "f3", "source": "gateway1", "target": "approve_leader",
             "condition": "days <= 3", "name": "3天以内"},
            {"id": "f4", "source": "gateway1", "target": "approve_hr",
             "condition": "days > 3", "name": "超过3天"},
            {"id": "f5", "source": "approve_leader", "target": "end"},
            {"id": "f6", "source": "approve_hr", "target": "end"}
        ]
    }
    """

    NODE_TYPE_MAP = {
        "startEvent": NodeType.START_EVENT,
        "endEvent": NodeType.END_EVENT,
        "task": NodeType.TASK,
        "userTask": NodeType.USER_TASK,
        "serviceTask": NodeType.SERVICE_TASK,
        "exclusiveGateway": NodeType.EXCLUSIVE_GATEWAY,
        "parallelGateway": NodeType.PARALLEL_GATEWAY,
        "timerEvent": NodeType.TIMER_EVENT,
        "intermediateEvent": NodeType.INTERMEDIATE_EVENT,
    }

    @classmethod
    def from_json(cls, json_str: str) -> ProcessDefinition:
        data = json.loads(json_str)
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ProcessDefinition:
        process_id = data.get("id") or data.get("processId")
        name = data.get("name", process_id)

        if not process_id:
            raise ValueError("流程定义必须包含 id 字段")

        nodes = cls._parse_nodes(data.get("nodes", []))
        flows = cls._parse_flows(data.get("flows", []))

        cls._validate_definition(nodes, flows)

        return ProcessDefinition(
            id=process_id,
            name=name,
            nodes={n.id: n for n in nodes},
            flows=flows,
        )

    @classmethod
    def from_file(cls, file_path: str) -> ProcessDefinition:
        with open(file_path, "r", encoding="utf-8") as f:
            if file_path.endswith(".json"):
                return cls.from_json(f.read())
            else:
                raise ValueError(f"不支持的文件格式: {file_path}")

    @classmethod
    def _parse_nodes(cls, nodes_data: List[Dict[str, Any]]) -> List[Node]:
        nodes = []
        for node_data in nodes_data:
            node_id = node_data.get("id")
            node_name = node_data.get("name", node_id)
            type_str = node_data.get("type")

            if not node_id or not type_str:
                raise ValueError(f"节点定义必须包含 id 和 type: {node_data}")

            node_type = cls.NODE_TYPE_MAP.get(type_str)
            if not node_type:
                raise ValueError(f"未知的节点类型: {type_str}")

            properties = {k: v for k, v in node_data.items()
                          if k not in ("id", "name", "type")}

            nodes.append(Node(
                id=node_id,
                name=node_name,
                node_type=node_type,
                properties=properties,
            ))
        return nodes

    @classmethod
    def _parse_flows(cls, flows_data: List[Dict[str, Any]]) -> List[SequenceFlow]:
        flows = []
        for flow_data in flows_data:
            flow_id = flow_data.get("id")
            source = flow_data.get("source") or flow_data.get("sourceRef")
            target = flow_data.get("target") or flow_data.get("targetRef")
            condition = flow_data.get("condition") or flow_data.get("conditionExpression")
            name = flow_data.get("name")

            if not flow_id or not source or not target:
                raise ValueError(f"流转定义必须包含 id, source, target: {flow_data}")

            flows.append(SequenceFlow(
                id=flow_id,
                source_id=source,
                target_id=target,
                condition=condition,
                name=name,
            ))
        return flows

    @classmethod
    def _validate_definition(cls, nodes: List[Node], flows: List[SequenceFlow]) -> None:
        node_ids = {n.id for n in nodes}

        start_nodes = [n for n in nodes if n.node_type == NodeType.START_EVENT]
        if len(start_nodes) == 0:
            raise ValueError("流程定义必须包含至少一个开始事件")

        end_nodes = [n for n in nodes if n.node_type == NodeType.END_EVENT]
        if len(end_nodes) == 0:
            raise ValueError("流程定义必须包含至少一个结束事件")

        for flow in flows:
            if flow.source_id not in node_ids:
                raise ValueError(f"流转 {flow.id} 的源节点不存在: {flow.source_id}")
            if flow.target_id not in node_ids:
                raise ValueError(f"流转 {flow.id} 的目标节点不存在: {flow.target_id}")
