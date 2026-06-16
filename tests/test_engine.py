"""
BPMN 工作流引擎测试用例
覆盖: 排他网关、并行网关、人工任务挂起/恢复、定时事件、持久化恢复
"""

import os
import sys
import time
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from workflow_engine import (
    ProcessParser, WorkflowEngine, PersistenceManager,
    TaskManager, EventScheduler, ProcessStatus, TokenStatus,
    TaskStatus, TimerStatus,
)


def get_example_path(name):
    return os.path.join(os.path.dirname(__file__), "..", "examples", name)


class TestExclusiveGateway:
    """测试排他网关(XOR): 按条件选择单条路径"""

    def setup_method(self):
        self.temp_db = tempfile.mktemp(suffix=".db")
        self.persistence = PersistenceManager(self.temp_db)
        self.task_manager = TaskManager(self.persistence)
        self.scheduler = EventScheduler(self.persistence)
        self.engine = WorkflowEngine(
            persistence=self.persistence,
            task_manager=self.task_manager,
            scheduler=self.scheduler,
        )
        definition = ProcessParser.from_file(get_example_path("leave_approval.json"))
        self.engine.register_definition(definition)

        self.notifications = {"reject": 0, "approve": 0}

        def send_reject(ctx):
            self.notifications["reject"] += 1
            return {}

        def send_approve(ctx):
            self.notifications["approve"] += 1
            return {}

        self.engine.register_service_handler("send_reject_notification", send_reject)
        self.engine.register_service_handler("send_approve_notification", send_approve)

    def teardown_method(self):
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)

    def test_short_leave_goes_to_leader(self):
        """请假<=3天, 走主管审批路径, 审批通过"""
        instance = self.engine.start_process("leave_approval", {"days": 2})

        tasks = self.task_manager.list_pending_tasks("employee")
        assert len(tasks) == 1, f"应有1个员工任务, 实际{len(tasks)}"
        assert tasks[0].task_name == "提交请假申请"

        self.engine.complete_task(tasks[0].id, {})

        leader_tasks = self.task_manager.list_pending_tasks("leader")
        assert len(leader_tasks) == 1, f"应有1个主管任务, 实际{len(leader_tasks)}"
        assert leader_tasks[0].task_name == "主管审批"

        hr_tasks = self.task_manager.list_pending_tasks("hr")
        assert len(hr_tasks) == 0, f"不应有HR任务, 实际{len(hr_tasks)}"

        self.engine.complete_task(leader_tasks[0].id, {"approved": True})

        instance = self.engine.get_instance(instance.id)
        assert instance.status == ProcessStatus.COMPLETED
        assert self.notifications["approve"] == 1
        assert self.notifications["reject"] == 0
        print("✓ 排他网关: 短请假走主管审批路径 通过")

    def test_long_leave_goes_to_hr(self):
        """请假>3天, 走HR审批路径, 审批拒绝"""
        instance = self.engine.start_process("leave_approval", {"days": 5})

        tasks = self.task_manager.list_pending_tasks("employee")
        self.engine.complete_task(tasks[0].id, {})

        hr_tasks = self.task_manager.list_pending_tasks("hr")
        assert len(hr_tasks) == 1, f"应有1个HR任务, 实际{len(hr_tasks)}"
        assert hr_tasks[0].task_name == "HR审批"

        leader_tasks = self.task_manager.list_pending_tasks("leader")
        assert len(leader_tasks) == 0, f"不应有主管任务, 实际{len(leader_tasks)}"

        self.engine.complete_task(hr_tasks[0].id, {"approved": False})

        instance = self.engine.get_instance(instance.id)
        assert instance.status == ProcessStatus.COMPLETED
        assert self.notifications["reject"] == 1
        assert self.notifications["approve"] == 0
        print("✓ 排他网关: 长请假走HR审批路径 通过")


class TestParallelGateway:
    """测试并行网关(AND): Fork多个Token并行, Join汇聚同步"""

    def setup_method(self):
        self.temp_db = tempfile.mktemp(suffix=".db")
        self.persistence = PersistenceManager(self.temp_db)
        self.task_manager = TaskManager(self.persistence)
        self.scheduler = EventScheduler(self.persistence)
        self.engine = WorkflowEngine(
            persistence=self.persistence,
            task_manager=self.task_manager,
            scheduler=self.scheduler,
        )
        definition = ProcessParser.from_file(get_example_path("order_processing.json"))
        self.engine.register_definition(definition)

        self.service_log = {"create_order": 0, "process_payment": 0,
                            "deduct_inventory": 0, "send_confirmation": 0}

        def log_service(name):
            def handler(ctx):
                self.service_log[name] += 1
                return {f"{name}_done": True}
            return handler

        for svc in self.service_log:
            self.engine.register_service_handler(svc, log_service(svc))

    def teardown_method(self):
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)

    def test_parallel_branch_fork(self):
        """并行网关分叉: 支付、库存、物流三个分支并行"""
        instance = self.engine.start_process("order_processing", {"order_id": "O001"})

        assert self.service_log["create_order"] == 1

        assert self.service_log["process_payment"] == 1
        assert self.service_log["deduct_inventory"] == 1

        warehouse_tasks = self.task_manager.list_pending_tasks("warehouse")
        assert len(warehouse_tasks) == 1, f"物流任务应存在, 实际{len(warehouse_tasks)}"
        assert warehouse_tasks[0].task_name == "物流调度"

        instance = self.engine.get_instance(instance.id)
        active_tokens = instance.get_active_tokens()
        assert len(active_tokens) >= 1

        print("✓ 并行网关: 分叉出3个并行分支 通过")

    def test_parallel_join_sync(self):
        """并行网关汇聚: 所有分支完成后才能继续, 不能提前汇合"""
        instance = self.engine.start_process("order_processing", {"order_id": "O002"})

        instance = self.engine.get_instance(instance.id)
        assert self.service_log["send_confirmation"] == 0, "汇聚未完成, 不应执行发送确认"

        warehouse_tasks = self.task_manager.list_pending_tasks("warehouse")
        assert len(warehouse_tasks) == 1

        self.engine.complete_task(warehouse_tasks[0].id, {"tracking_no": "SF12345"})

        pending = self.scheduler.get_pending_timers()
        assert len(pending) >= 1, "汇聚完成后应创建定时事件"

        print("✓ 并行网关: 所有分支到达后才汇聚 通过")

    def test_join_waits_all_branches(self):
        """验证并行汇聚不会提前触发: 支付+库存完成但物流未完成时, 流程不继续"""
        self.engine.register_service_handler("process_payment", lambda ctx: {})
        self.engine.register_service_handler("deduct_inventory", lambda ctx: {})

        instance = self.engine.start_process("order_processing", {"order_id": "O003"})

        warehouse_tasks = self.task_manager.list_pending_tasks("warehouse")
        assert len(warehouse_tasks) == 1, "物流任务存在"

        pending = self.scheduler.get_pending_timers()
        assert len(pending) == 0, "物流任务未完成, 汇聚未触发, 不应有定时器"

        self.engine.complete_task(warehouse_tasks[0].id, {})

        pending = self.scheduler.get_pending_timers()
        assert len(pending) == 1, "所有分支完成, 汇聚触发, 创建定时器"
        print("✓ 并行网关: 汇聚正确等待所有分支 不提前汇合 通过")


class TestUserTaskSuspend:
    """测试人工任务: 流程挂起等待, 完成后恢复"""

    def setup_method(self):
        self.temp_db = tempfile.mktemp(suffix=".db")
        self.persistence = PersistenceManager(self.temp_db)
        self.task_manager = TaskManager(self.persistence)
        self.engine = WorkflowEngine(
            persistence=self.persistence,
            task_manager=self.task_manager,
        )
        definition = ProcessParser.from_file(get_example_path("leave_approval.json"))
        self.engine.register_definition(definition)
        self.engine.register_service_handler("send_reject_notification", lambda ctx: {})
        self.engine.register_service_handler("send_approve_notification", lambda ctx: {})

    def teardown_method(self):
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)

    def test_task_suspends_process(self):
        """人工任务使流程挂起, Token状态为SUSPENDED"""
        instance = self.engine.start_process("leave_approval", {"days": 2})

        instance = self.engine.get_instance(instance.id)
        assert instance.status == ProcessStatus.RUNNING

        active = instance.get_active_tokens()
        suspended = [t for t in instance.tokens.values() if t.status == TokenStatus.SUSPENDED]
        assert len(suspended) == 1, f"应有1个SUSPENDED Token, 实际{len(suspended)}"
        print("✓ 人工任务: Token挂起SUSPENDED状态 通过")

    def test_task_completion_resumes(self):
        """完成人工任务后流程恢复并继续执行"""
        instance = self.engine.start_process("leave_approval", {"days": 5})

        tasks = self.task_manager.list_pending_tasks("employee")
        assert len(tasks) == 1
        assert tasks[0].status == TaskStatus.ASSIGNED

        self.engine.complete_task(tasks[0].id, {"reason": "年假"})

        instance = self.engine.get_instance(instance.id)
        hr_tasks = self.task_manager.list_pending_tasks("hr")
        assert len(hr_tasks) == 1, "流程继续执行到HR审批"

        self.engine.complete_task(hr_tasks[0].id, {"approved": True})

        instance = self.engine.get_instance(instance.id)
        assert instance.status == ProcessStatus.COMPLETED
        print("✓ 人工任务: 完成后恢复流程执行 通过")

    def test_task_form_data_persisted(self):
        """人工任务的表单数据持久化并传递到后续节点"""
        instance = self.engine.start_process("leave_approval", {"days": 5})
        tasks = self.task_manager.list_pending_tasks("employee")
        self.engine.complete_task(tasks[0].id, {"reason": "年假", "start_date": "2026-07-01"})

        task = self.task_manager.get_task(tasks[0].id)
        assert task.form_data.get("reason") == "年假"
        assert task.status == TaskStatus.COMPLETED

        hr_tasks = self.task_manager.list_pending_tasks("hr")
        self.engine.complete_task(hr_tasks[0].id, {"approved": True, "comment": "已批准"})

        instance = self.engine.get_instance(instance.id)
        assert instance.context.get("reason") == "年假"
        assert instance.context.get("approved") is True
        print("✓ 人工任务: 表单数据持久化和上下文传递 通过")


class TestPersistenceRecovery:
    """测试持久化与服务重启恢复"""

    def setup_method(self):
        self.temp_db = tempfile.mktemp(suffix=".db")

    def teardown_method(self):
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)

    def test_instance_persisted_and_reloaded(self):
        """流程实例状态持久化, 可从数据库重新加载"""
        persistence = PersistenceManager(self.temp_db)
        task_manager = TaskManager(persistence)
        engine = WorkflowEngine(persistence=persistence, task_manager=task_manager)

        definition = ProcessParser.from_file(get_example_path("leave_approval.json"))
        engine.register_definition(definition)
        engine.register_service_handler("send_reject_notification", lambda ctx: {})
        engine.register_service_handler("send_approve_notification", lambda ctx: {})

        instance_id = engine.start_process("leave_approval", {"days": 3, "user": "张三"}).id

        del engine
        del task_manager
        del persistence

        persistence2 = PersistenceManager(self.temp_db)
        task_manager2 = TaskManager(persistence2)
        engine2 = WorkflowEngine(persistence=persistence2, task_manager=task_manager2)
        definition2 = ProcessParser.from_file(get_example_path("leave_approval.json"))
        engine2.register_definition(definition2)
        engine2.register_service_handler("send_reject_notification", lambda ctx: {})
        engine2.register_service_handler("send_approve_notification", lambda ctx: {})

        reloaded = engine2.get_instance(instance_id)
        assert reloaded is not None
        assert reloaded.definition_id == "leave_approval"
        assert reloaded.context.get("days") == 3
        assert reloaded.context.get("user") == "张三"
        assert reloaded.status == ProcessStatus.RUNNING

        tasks = task_manager2.list_pending_tasks("employee")
        assert len(tasks) == 1

        engine2.complete_task(tasks[0].id, {})
        leader_tasks = task_manager2.list_pending_tasks("leader")
        assert len(leader_tasks) == 1

        engine2.complete_task(leader_tasks[0].id, {"approved": True})

        reloaded = engine2.get_instance(instance_id)
        assert reloaded.status == ProcessStatus.COMPLETED
        print("✓ 持久化: 服务重启后流程状态恢复并继续执行 通过")

    def test_resume_all_running_instances(self):
        """resume_all恢复所有运行中的实例"""
        persistence = PersistenceManager(self.temp_db)
        task_manager = TaskManager(persistence)
        engine = WorkflowEngine(persistence=persistence, task_manager=task_manager)

        definition = ProcessParser.from_file(get_example_path("leave_approval.json"))
        engine.register_definition(definition)
        engine.register_service_handler("send_reject_notification", lambda ctx: {})
        engine.register_service_handler("send_approve_notification", lambda ctx: {})

        engine.start_process("leave_approval", {"days": 1})
        engine.start_process("leave_approval", {"days": 10})

        tasks = task_manager.list_pending_tasks("employee")
        for t in tasks:
            engine.complete_task(t.id, {})

        running = persistence.load_running_instances()
        assert len(running) == 2

        for inst in running:
            for t in inst.tokens.values():
                t.status = TokenStatus.ACTIVE
            persistence.save_instance(inst)

        engine.resume_all()

        running_after = persistence.load_running_instances()
        assert len(running_after) == 2, "流程都应挂在人工任务上, 保持RUNNING"
        print("✓ 持久化: resume_all批量恢复运行实例 通过")


class TestTimerEvent:
    """测试定时事件"""

    def setup_method(self):
        self.temp_db = tempfile.mktemp(suffix=".db")
        self.persistence = PersistenceManager(self.temp_db)
        self.task_manager = TaskManager(self.persistence)
        self.scheduler = EventScheduler(self.persistence, polling_interval=0.1)
        self.engine = WorkflowEngine(
            persistence=self.persistence,
            task_manager=self.task_manager,
            scheduler=self.scheduler,
        )
        self.scheduler.set_engine(self.engine)

        definition = ProcessParser.from_file(get_example_path("order_processing.json"))
        self.engine.register_definition(definition)
        for svc in ["create_order", "process_payment", "deduct_inventory", "send_confirmation"]:
            self.engine.register_service_handler(svc, lambda ctx: {})

    def teardown_method(self):
        self.scheduler.stop()
        if os.path.exists(self.temp_db):
            os.remove(self.temp_db)

    def test_timer_creates_waiting_token(self):
        """定时事件让Token变为WAITING, 创建TimerInstance"""
        instance = self.engine.start_process("order_processing", {"order_id": "T001"})

        warehouse_tasks = self.task_manager.list_pending_tasks("warehouse")
        self.engine.complete_task(warehouse_tasks[0].id, {})

        instance = self.engine.get_instance(instance.id)
        waiting_tokens = [t for t in instance.tokens.values() if t.status == TokenStatus.WAITING]
        assert len(waiting_tokens) >= 1, "定时事件触发后Token应为WAITING"

        timers = self.scheduler.get_pending_timers()
        assert len(timers) >= 1, "应创建TimerInstance"
        print("✓ 定时事件: Token WAITING状态 + Timer创建 通过")

    def test_timer_firing_resumes_process(self):
        """定时事件到期触发后流程继续执行"""
        instance = self.engine.start_process("order_processing", {"order_id": "T002"})

        warehouse_tasks = self.task_manager.list_pending_tasks("warehouse")
        self.engine.complete_task(warehouse_tasks[0].id, {})

        timers = self.scheduler.get_pending_timers()
        assert len(timers) == 1

        for t in timers:
            t.fire_time = time.time() - 1
            self.persistence.save_timer(t)

        fired = self.scheduler.tick()
        assert len(fired) >= 1, f"应触发至少1个定时器, 实际{len(fired)}"

        instance = self.engine.get_instance(instance.id)
        assert instance.status == ProcessStatus.COMPLETED, f"流程应已完成, 实际{instance.status}"
        print("✓ 定时事件: 触发后恢复流程执行并完成 通过")


def run_all_tests():
    print("\n" + "=" * 60)
    print("开始运行 BPMN 工作流引擎测试用例")
    print("=" * 60 + "\n")

    test_classes = [
        TestExclusiveGateway,
        TestParallelGateway,
        TestUserTaskSuspend,
        TestPersistenceRecovery,
        TestTimerEvent,
    ]

    passed = 0
    failed = 0

    for cls in test_classes:
        print(f"\n--- {cls.__name__} ---")
        instance = cls()
        methods = [m for m in dir(instance) if m.startswith("test_")]
        for method_name in methods:
            try:
                instance.setup_method()
                getattr(instance, method_name)()
                passed += 1
            except Exception as e:
                failed += 1
                print(f"✗ {method_name}: {e}")
                import traceback
                traceback.print_exc()
            finally:
                try:
                    instance.teardown_method()
                except:
                    pass

    print("\n" + "=" * 60)
    print(f"测试完成: 通过 {passed}, 失败 {failed}")
    print("=" * 60)
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
