"""
测试 contextvars 的线程/协程隔离性

问题：
- Thread A 设置的值，Thread B 能看到吗？
- Task A 设置的值，Task B 能看到吗？
- run_in_executor 中的协程能看到上下文吗？
"""

import contextvars
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor
import time


# 核心变量
_flag: contextvars.ContextVar[bool] = contextvars.ContextVar('flag', default=False)


def print_status(label: str):
    """打印当前状态"""
    value = _flag.get()
    thread_id = threading.current_thread().ident
    try:
        task_id = id(asyncio.current_task())
    except RuntimeError:
        task_id = None
    print(f"[{label}] Thread={thread_id[:8]} Task={task_id} flag={value}")


def thread_a_work():
    """线程 A 的工作"""
    print_status("Thread-A-Start")
    _flag.set(True)
    time.sleep(0.5)  # 等待
    print_status("Thread-A-AfterSet")
    print(f"  → Thread A 设置了 flag=True，但 Thread B 看不到！")


def thread_b_work():
    """线程 B 的工作（并发执行）"""
    time.sleep(0.1)  # 稍微延迟启动
    print_status("Thread-B-Check")
    print(f"  → Thread B 检查到的 flag={_flag.get()}（不受 Thread A 影响）")


async def task_a_work():
    """协程 Task A 的工作"""
    print_status("Task-A-Start")
    _flag.set(True)
    await asyncio.sleep(0.5)
    print_status("Task-A-AfterSet")


async def task_b_work():
    """协程 Task B 的工作（并发执行）"""
    await asyncio.sleep(0.1)
    print_status("Task-B-Check")
    print(f"  → Task B 检查到的 flag={_flag.get()}（不受 Task A 影响）")


async def executor_work():
    """在线程池中执行的工作"""
    print_status("Executor-Start")
    await asyncio.sleep(0.1)
    print_status("Executor-Check")
    print(f"  → 线程池中的任务检查到的 flag={_flag.get()}")


def test_thread_isolation():
    """测试线程隔离"""
    print("=" * 60)
    print("【测试 1: 线程隔离】")
    print("=" * 60)
    
    # 启动两个线程
    t1 = threading.Thread(target=thread_a_work, name="Thread-A")
    t2 = threading.Thread(target=thread_b_work, name="Thread-B")
    
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()
    
    print("  → 结论：不同线程的 contextvars 完全隔离")


async def test_coroutine_isolation():
    """测试协程隔离"""
    print("\n" + "=" * 60)
    print("【测试 2: 协程隔离（同一线程）】")
    print("=" * 60)
    
    # 并发执行两个协程
    await asyncio.gather(
        task_a_work(),
        task_b_work()
    )
    
    print("  → 结论：同一线程内的不同 Task 上下文完全隔离")


async def test_executor_context():
    """测试线程池中的上下文传递"""
    print("\n" + "=" * 60)
    print("【测试 3: run_in_executor 中的上下文】")
    print("=" * 60)
    
    # 设置 flag
    _flag.set(True)
    print_status("Main-BeforeExecutor")
    
    # 在线程池中执行
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=1) as executor:
        await loop.run_in_executor(executor, time.sleep, 0.1)
        # 在线程池中检查
        result = await loop.run_in_executor(executor, _flag.get)
        print(f"  → 线程池中检查到的 flag={result}")


async def test_nested_context():
    """测试嵌套上下文"""
    print("\n" + "=" * 60)
    print("【测试 4: 嵌套上下文（Token 恢复）】")
    print("=" * 60)
    
    # 主上下文
    token1 = _flag.set(True)
    print_status("Level-1")
    
    async def level2():
        # 嵌套上下文
        token2 = _flag.set(False)
        print_status("Level-2")
        await asyncio.sleep(0.1)
        print_status("Level-2-BeforeExit")
        _flag.reset(token2)  # 恢复到 Level-1
        print_status("Level-2-AfterExit")
    
    await level2()
    
    # 回到 Level-1
    _flag.reset(token1)
    print_status("Level-1-AfterReset")
    print("  → 嵌套退出后正确恢复到外层上下文")


async def test_full_workflow():
    """模拟完整工作流"""
    print("\n" + "=" * 60)
    print("【完整工作流模拟】")
    print("=" * 60)
    
    async def handle():
        """handle 阶段 - 启用 RASP"""
        token = _flag.set(True)
        print_status("Handle-Start")
        try:
            await asyncio.sleep(0.2)
            print_status("Handle-Middle")
            await asyncio.sleep(0.2)
            print_status("Handle-End")
        finally:
            _flag.reset(token)
    
    async def execute():
        """execute 阶段 - 禁用 RASP"""
        print_status("Execute-Check")
        await asyncio.sleep(0.1)
        print_status("Execute-Done")
    
    async def other_task():
        """其他并发任务"""
        print_status("OtherTask-Check")
    
    # 并发执行
    await asyncio.gather(
        handle(),
        execute(),
        other_task()
    )
    
    print("\n  → handle 中启用 RASP，不影响 execute 和 other_task")


async def main():
    await test_thread_isolation()
    await test_coroutine_isolation()
    await test_executor_context()
    await test_nested_context()
    await test_full_workflow()
    
    print("\n" + "=" * 60)
    print("【总结】")
    print("=" * 60)
    print("""
┌─────────────────┬──────────────────────────────────────────────┐
│ 场景            │ 是否隔离                                     │
├─────────────────┼──────────────────────────────────────────────┤
│ 不同线程        │ ✓ 完全隔离                                   │
│ 同一线程不同    │ ✓ 完全隔离（contextvars 核心优势）           │
│   asyncio.Task  │                                              │
│ run_in_executor │ ⚠️ 取决于实现，通常会传递父上下文            │
│ 嵌套上下文      │ ✓ 使用 Token 正确恢复                       │
└─────────────────┴──────────────────────────────────────────────┘

结论：
1. contextvars 为每个线程和每个 asyncio.Task 提供独立上下文
2. Thread A 无法看到 Thread B 设置的值
3. Task A 无法看到 Task B 设置的值
4. 这是 contextvars 设计的核心目标
""")


if __name__ == "__main__":
    asyncio.run(main())
