# BPMN 工作流引擎架构说明

## 项目结构

```
6/
├── workflow_engine/          # 引擎核心包
│   ├── __init__.py           # 模块导出
│   ├── models.py             # 核心数据模型 (流程图、节点、Token、实例)
│   ├── parser.py             # 流程定义解析 (JSON/BPMN)
│   ├── engine.py             # 执行引擎 (Token 流转、网关处理)
│   ├── persistence.py        # 状态持久化 (SQLite)
│   ├── task_manager.py       # 人工任务管理 (挂起/恢复)
│   └── scheduler.py          # 事件调度 (定时事件)
├── examples/                 # 示例流程定义
│   ├── leave_approval.json   # 请假审批 (排他网关)
│   └── order_processing.json # 订单处理 (并行网关+定时事件)
└── tests/
    └── test_engine.py        # 完整测试用例 (12个测试全部通过)
```

---

## 一、流程建模：有向图 + Token 流转驱动

### 1.1 有向图模型

流程本质上是一张**有向图 (Directed Graph)**：

- **顶点 (Node/节点)**：
  - 事件：`StartEvent`(开始)、`EndEvent`(结束)、`TimerEvent`(定时)
  - 任务：`Task`(自动)、`UserTask`(人工)、`ServiceTask`(服务调用)
  - 网关：`ExclusiveGateway`(排他/XOR)、`ParallelGateway`(并行/AND)

- **有向边 (SequenceFlow/流转)**：连接两个节点，可携带条件表达式

数据结构定义见 [models.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/models.py#L114-L135)：

```python
@dataclass
class ProcessDefinition:
    id: str
    name: str
    nodes: Dict[str, Node]              # 顶点集合
    flows: List[SequenceFlow]           # 边集合

@dataclass
class SequenceFlow:
    id: str
    source_id: str                       # 源节点
    target_id: str                       # 目标节点
    condition: Optional[str] = None      # 条件表达式 (排他网关用)
```

### 1.2 Token 驱动机制

**Token 是流程的执行单元**，代表一条执行路径上的"令牌"。

核心思想来自 Petri 网和 BPMN 规范：
- 单路径流程中只有 **1 个 Token** 沿节点串行移动
- 并行网关处 **1 个 Token 分叉(Fork)出 N 个 Token** 并行执行
- 并行汇聚处 **N 个 Token 合并(Join)为 1 个 Token** 继续执行

Token 定义见 [models.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/models.py#L66-L90)：

```python
@dataclass
class Token:
    id: str
    instance_id: str
    current_node_id: Optional[str]       # 当前所在节点
    status: TokenStatus                  # ACTIVE/WAITING/SUSPENDED/COMPLETED/CONSUMED
    context: Dict[str, Any]              # 该分支的局部上下文
    parent_token_id: Optional[str]       # 父Token (分叉溯源)
    fork_id: Optional[str]               # 分叉批次ID (同批次用于汇聚识别)
```

### 1.3 执行循环

引擎在 `_continue_process()` 中实现执行循环，见 [engine.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/engine.py#L153-L180)：

```
while 存在 ACTIVE 状态的 Token:
    for 每个 ACTIVE Token:
        获取 Token 所在节点 → 根据节点类型处理
            StartEvent      → 直接流向下一节点
            EndEvent        → Token 标记 COMPLETED
            Task/ServiceTask→ 执行逻辑后流向下一节点
            UserTask        → Token SUSPENDED, 创建任务, 等待外部
            ExclusiveGateway→ 按条件选一条出边
            ParallelGateway → Fork 或 Join
            TimerEvent      → Token WAITING, 创建定时器, 等待调度
    检查是否所有 Token 都已结束 → 标记流程 COMPLETED
```

---

## 二、排他网关 (XOR Gateway)：按条件选单条路径

### 2.1 工作原理

排他网关保证**从多条出边中选择且仅选择一条路径**。

实现逻辑见 [engine.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/engine.py#L289-L330)：

```python
def _handle_exclusive_gateway(self, instance, token, node, definition):
    flows = definition.get_outgoing_flows(node.id)  # 获取所有出边
    context = {**instance.context, **token.context}

    for flow in flows:
        if flow.condition:
            # 按顺序求值每条边的条件表达式
            if self.condition_evaluator.evaluate(flow.condition, context):
                selected_flow = flow
                break
        else:
            # 没有条件的边作为默认候选
            if not default_flow_id or flow.id == default_flow_id:
                selected_flow = flow

    token.current_node_id = selected_flow.target_id  # Token 沿选中边前进
```

### 2.2 示例：请假审批流程

定义见 [leave_approval.json](file:///d:/trae-bz/TraeProjects/6/examples/leave_approval.json)：

```
开始 → 提交申请 → [天数判断] ──days<=3──→ 主管审批 ──┐
                          └──days>3───→ HR审批  ──┘
                                              ↓
                                          [审批结果] ──approved=True──→ 通知通过 → 结束
                                                        └──approved=False──→ 通知拒绝 → 结束
```

**关键点**：
- 条件表达式如 `"days <= 3"` 使用 Python 表达式求值，可引用上下文变量
- 可配置 `default` 指定默认出边，所有条件都不满足时走默认路径
- Token 始终只有**一个**，不会产生并行分支

---

## 三、并行网关 (AND Gateway)：分叉与汇聚同步

### 3.1 分叉 (Fork)：一个 Token → 多个并发 Token

当 Token 到达并行网关且该网关有多条出边时：
- 原 Token 标记为 `CONSUMED`（已消费）
- 为每条出边创建一个新的 Token，携带相同的 `fork_id` 和上下文副本
- 多个新 Token 进入各自分支，独立并行执行

实现代码见 [engine.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/engine.py#L368-L381)：

```python
# 分叉逻辑: 1个Token变成N个Token
token.status = TokenStatus.CONSUMED
for flow in outgoing_flows:
    child = Token.create(
        instance.id, flow.target_id,
        dict(token.context),           # 上下文副本, 分支独立
        parent_token_id=token.id,
        fork_id=fork_id,               # 相同fork_id标记为同批次
    )
    instance.tokens[child.id] = child
```

### 3.2 汇聚 (Join)：多个 Token → 一个 Token

**这是并行网关正确性的核心：如何确保不提前汇合、不重复汇合？**

实现采用 **fork_id + 到达计数** 机制，见 [engine.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/engine.py#L333-L367)：

```python
def _handle_parallel_gateway(self, instance, token, node, definition):
    incoming_flows = definition.get_incoming_flows(node.id)  # 该网关的入边数
    fork_id = token.fork_id                                    # 当前Token所属的分叉批次

    # 1. 统计同 fork_id 下已到达该网关的 Token 数
    tokens_at_node = instance.get_tokens_at_node(node.id)
    arrived_tokens = [t for t in tokens_at_node
                      if t.fork_id == fork_id and t.id != token.id]
    arrived_count = len(arrived_tokens) + 1  # +1 包括当前Token

    # 2. 到达数 < 入边数 → 还没到齐, 当前 Token 等待
    if arrived_count < len(incoming_flows):
        token.status = TokenStatus.WAITING
        token.fork_id = fork_id
        return True

    # 3. 到达数 == 入边数 → 全部到齐, 可以汇聚
    for t in arrived_tokens:
        t.status = TokenStatus.CONSUMED     # 已消费, 不会再次参与汇聚
    token.status = TokenStatus.CONSUMED

    # 4. 合并所有分支的上下文, 创建新 Token 继续前进
    merged_context = merge(token.context, arrived_tokens[*].context)
    new_token = Token.create(instance.id, next_node_id, merged_context, fork_id=fork_id)
    instance.tokens[new_token.id] = new_token
```

### 3.3 防止提前/重复汇合的三重保障

| 保障机制 | 说明 |
|---------|------|
| **fork_id 隔离** | 同批次分叉的 Token 共享 fork_id，不同批次的 Token 不会互相干扰。即使同一个网关有多组并行分支同时到达，也只合并同 fork_id 的组 |
| **到达数 == 入边数** | 必须等所有入边都有对应 Token 到达才触发汇聚。如果有 3 条入边，必须集齐 3 个同 fork_id 的 Token |
| **CONSUMED 标记** | 已参与汇聚的 Token 状态变为 CONSUMED，在后续计数时被排除，不会重复参与汇聚 |

### 3.4 示例：订单处理流程

定义见 [order_processing.json](file:///d:/trae-bz/TraeProjects/6/examples/order_processing.json)：

```
开始 → 创建订单 → [并行开始] ──┬──→ 支付处理 ──┐
                                ├──→ 库存扣减 ──┤
                                └──→ 物流调度(人工) ─┘
                                                  ↓
                                          [并行结束] → 等待发货(2秒定时器) → 发送确认 → 结束
```

测试中验证了：
- 支付、库存自动执行，物流调度为人工任务挂起
- 在物流任务未完成前，汇聚不触发（无定时器创建）
- 完成物流任务后，3个分支全部到达，汇聚触发，定时器创建

---

## 四、人工任务：挂起等待与外部恢复

### 4.1 挂起机制

当 Token 到达 `UserTask` 时，执行以下操作，见 [engine.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/engine.py#L265-L295)：

1. **Token 状态变更**：`ACTIVE` → `SUSPENDED`
2. **创建 TaskInstance**：记录任务名、办理人、关联的 Token ID 和流程实例 ID
3. **持久化**：Token 状态和 TaskInstance 都写入数据库
4. **执行循环中断**：该分支不再前进，等待外部触发

```python
def _handle_user_task(self, instance, token, node):
    token.status = TokenStatus.SUSPENDED          # Token 挂起
    task = TaskInstance.create(
        instance_id=instance.id,
        token_id=token.id,
        node_id=node.id,
        task_name=node.name,
    )
    task.assignee = node.get("assignee")          # 指定办理人
    self.task_manager.create_task(task)           # 持久化任务
    return True
```

### 4.2 恢复机制

外部系统/用户完成任务时调用 `complete_task()`，见 [engine.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/engine.py#L100-L120)：

1. **更新任务状态**：TaskInstance → `COMPLETED`，保存表单数据
2. **Token 前进**：使用 `_advance_from_current()` 让 Token 直接离开 UserTask 节点到下一个节点
3. **Token 状态恢复**：`SUSPENDED` → `ACTIVE`
4. **上下文合并**：表单数据合并到 Token 和实例的 context
5. **继续执行**：调用 `_continue_process()` 驱动流程前进

```python
def complete_task(self, task_id, form_data=None):
    task = self.task_manager.complete_task(task_id, form_data)
    instance = self._load_instance(task.instance_id)
    token = instance.tokens[task.token_id]

    token.context.update(form_data)           # 合并表单数据
    instance.context.update(form_data)

    self._advance_from_current(instance, token, definition)  # 先前进到下一节点
    token.status = TokenStatus.ACTIVE
    self._persist_instance(instance)
    self._continue_process(instance)          # 恢复执行
```

**注意**：必须先 `_advance_from_current()` 让 Token 离开 UserTask，否则执行循环会再次进入 `_handle_user_task()` 又把 Token 挂起。

### 4.3 数据持久化

所有状态都持久化到 SQLite，表结构见 [persistence.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/persistence.py#L40-L93)：
- `process_instances`：流程实例状态 + 全局上下文
- `tokens`：每个 Token 的状态、位置、上下文
- `task_instances`：人工任务状态、办理人、表单数据

服务重启后通过 `resume_all()` 即可恢复所有挂起的流程。

---

## 五、状态持久化：服务重启不丢失

### 5.1 持久化策略

采用 **状态变更即持久化** 的策略：
- 流程实例创建时保存
- 每次 Token 状态变更后保存整个实例（含所有 Token）
- 人工任务/定时事件创建、状态变更时单独保存

核心方法见 [persistence.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/persistence.py#L141-L188)：

```python
def save_instance(self, instance: ProcessInstance):
    # 1. 保存/更新实例主记录
    conn.execute("INSERT OR REPLACE INTO process_instances (...) VALUES (...)",
                 (instance.id, instance.definition_id, instance.status.value,
                  json.dumps(instance.context), ...))

    # 2. 先删后插保存所有 Token (全量覆盖)
    conn.execute("DELETE FROM tokens WHERE instance_id = ?", (instance.id,))
    for token in instance.tokens.values():
        conn.execute("INSERT INTO tokens (...) VALUES (...)",
                     (token.id, token.instance_id, token.current_node_id,
                      token.status.value, json.dumps(token.context), ...))
```

### 5.2 恢复机制

服务重启后的恢复流程：

```python
# 1. 从数据库加载所有 RUNNING 状态的实例
instances = persistence.load_running_instances()

# 2. 对每个实例重新驱动执行
for instance in instances:
    engine._continue_process(instance)
```

ACTIVE 状态的 Token 会自动继续流转；
SUSPENDED（等待人工任务）和 WAITING（等待定时事件）的 Token 保持原状，等待外部触发。

持久化模块测试见 [test_engine.py](file:///d:/trae-bz/TraeProjects/6/tests/test_engine.py) 中的 `TestPersistenceRecovery`，验证了：
- 模拟 Engine/TM/Persistence 全部销毁重建
- 从数据库重新加载流程实例，状态、上下文完整保留
- 人工任务可以继续完成，流程正常执行到结束

---

## 六、定时事件：调度触发

### 6.1 定时事件注册

当 Token 到达 `TimerEvent` 时：
1. Token 状态：`ACTIVE` → `WAITING`
2. 创建 `TimerInstance`，记录 `fire_time`（触发时间戳）
3. 注册到调度器 + 持久化

实现见 [engine.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/engine.py#L383-L417)：

```python
def _handle_timer_event(self, instance, token, node):
    token.status = TokenStatus.WAITING
    duration_seconds = self._parse_duration(node.get("duration"))  # "30s"/"5m"/"2h"/"1d"
    fire_time = time.time() + duration_seconds

    timer = TimerInstance.create(
        instance_id=instance.id, token_id=token.id, node_id=node.id,
        fire_time=fire_time, duration_seconds=duration_seconds,
    )
    self.scheduler.schedule_timer(timer)
    return True
```

### 6.2 调度器触发

调度器 `EventScheduler` 有两种运行模式，见 [scheduler.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/scheduler.py)：

**模式一：后台线程自动轮询**（生产环境）
```python
scheduler.start()   # 启动后台线程，每 polling_interval 秒扫描一次
```

**模式二：手动 tick**（测试/调试）
```python
scheduler.tick()    # 手动触发一次扫描
```

扫描逻辑：
```python
def tick(self):
    now = time.time()
    # 1. 从数据库同步最新待触发的定时器
    db_pending = self.persistence.load_pending_timers()

    # 2. 筛选 fire_time <= now 的定时器
    to_fire = [t for t in all_timers
               if t.status == PENDING and t.fire_time <= now]

    # 3. 逐个触发：通知 Engine 恢复对应 Token
    for timer in to_fire:
        self._engine.trigger_timer(timer.id)
```

### 6.3 触发后恢复流程

`trigger_timer()` 的逻辑与 `complete_task()` 类似：
- TimerInstance 标记为 `FIRED`
- Token 从 TimerEvent 节点前进到下一个节点
- Token 状态：`WAITING` → `ACTIVE`
- 调用 `_continue_process()` 继续执行

---

## 七、并发分支汇合正确性总结

并行网关汇合的完整正确性保障：

```
            [并行分叉] fork_id=F1
           /      |      \
      TokenA    TokenB    TokenC
          |         |         |
      (WAITING) (WAITING)   人工任务(SUSPENDED)
          |         |         |
           \        |        /
            [并行汇聚]
                │
                ▼
         1. 统计同 fork_id=F1 且到达该网关的 Token 数
         2. 到达数 < 入边数(3) → 继续等待 (TokenA/B 标记WAITING)
         3. 人工任务完成 → TokenC 到达
         4. 此时到达数 == 3 → 触发汇聚
         5. TokenA/B/C 全部标记 CONSUMED (防止重复参与)
         6. 合并上下文 → 创建新 Token → 继续前进
```

三重防错机制：
1. **fork_id** 隔离不同批次的并行分支（嵌套并行也正确）
2. **到达计数 == 入边数** 保证不提前汇合
3. **CONSUMED 标记** 保证不重复汇合

---

## 八、运行测试

```bash
cd d:\trae-bz\TraeProjects\6
python tests/test_engine.py
```

测试输出：
```
============================================================
开始运行 BPMN 工作流引擎测试用例
============================================================

--- TestExclusiveGateway ---
✓ 排他网关: 长请假走HR审批路径 通过
✓ 排他网关: 短请假走主管审批路径 通过

--- TestParallelGateway ---
✓ 并行网关: 汇聚正确等待所有分支 不提前汇合 通过
✓ 并行网关: 分叉出3个并行分支 通过
✓ 并行网关: 所有分支到达后才汇聚 通过

--- TestUserTaskSuspend ---
✓ 人工任务: 完成后恢复流程执行 通过
✓ 人工任务: 表单数据持久化和上下文传递 通过
✓ 人工任务: Token挂起SUSPENDED状态 通过

--- TestPersistenceRecovery ---
✓ 持久化: 服务重启后流程状态恢复并继续执行 通过
✓ 持久化: resume_all批量恢复运行实例 通过

--- TestTimerEvent ---
✓ 定时事件: Token WAITING状态 + Timer创建 通过
✓ 定时事件: 触发后恢复流程执行并完成 通过

============================================================
测试完成: 通过 12, 失败 0
============================================================
```

---

## 九、模块职责总览

| 模块 | 文件 | 核心职责 |
|-----|------|---------|
| 核心模型 | [models.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/models.py) | 有向图节点/边、Token、流程/任务/定时器实例的类型定义 |
| 流程解析 | [parser.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/parser.py) | 从 JSON 解析流程定义为有向图结构，校验完整性 |
| 执行引擎 | [engine.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/engine.py) | Token 驱动循环、排他/并行网关逻辑、任务/定时器挂起点 |
| 状态持久化 | [persistence.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/persistence.py) | SQLite 存储实例/Token/任务/定时器，支持重启恢复 |
| 任务管理 | [task_manager.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/task_manager.py) | 人工任务的创建、分配、完成、取消生命周期管理 |
| 事件调度 | [scheduler.py](file:///d:/trae-bz/TraeProjects/6/workflow_engine/scheduler.py) | 定时事件的注册、扫描、触发（后台线程/手动） |
