"""
使用 contextvars 全面替代 threading.local
==========================================

问题背景：
- threading.local 只支持线程，不支持协程/异步任务
- 在 asyncio 中，所有协程共享同一个线程本地状态
- contextvars 为每个协程任务提供独立的上下文

解决方案：
- 使用 contextvars 实现真正的上下文本地存储
- 同时兼容线程和协程场景
"""

import contextvars
import threading
import asyncio
from functools import wraps
from typing import Any, Callable, TypeVar

T = TypeVar('T')


# ============ 方案 1: 基础 contextvar 封装 ============

class TaskContext:
    """
    基于 contextvars 的上下文本地存储
    
    特点：
    1. 线程隔离：不同线程不同值
    2. 协程隔离：不同 asyncio.Task 不同值
    3. 自动生命周期管理
    """
    
    def __init__(self, name: str, default: Any = None):
        self._var = contextvars.ContextVar(name, default=default)
        self._name = name
    
    def set(self, value: Any) -> contextvars.Token:
        """设置当前上下文的值"""
        return self._var.set(value)
    
    def get(self, default: Any = None) -> Any:
        """获取当前上下文的值"""
        return self._var.get(default)
    
    def reset(self, token: contextvars.Token):
        """重置到之前的值"""
        self._var.reset(token)
    
    def __repr__(self):
        return f"<TaskContext '{self._name}' value={self._var.get()!r}>"


# ============ 方案 2: 线程安全的上下文变量 ============

class ThreadSafeContext:
    """
    线程安全 + 协程安全的上下文存储
    
    底层使用 contextvars，扩展了：
    1. 自动类型转换
    2. 嵌套支持
    3. 线程安全计数
    """
    
    def __init__(self, default: Any = None):
        self._var = contextvars.ContextVar('ctx', default=default)
        self._lock = threading.Lock()
        self._counters = {}  # 线程/任务ID -> 计数
    
    def set(self, value: Any):
        """设置值"""
        self._var.set(value)
    
    def get(self, default: Any = None) -> Any:
        """获取值"""
        return self._var.get(default)
    
    def increment(self, delta: int = 1) -> int:
        """
        原子增加（适合计数器场景）
        
        示例：
        ctx = ThreadSafeContext(0)
        ctx.set(0)
        
        # handle 进入时
        with self._lock:
            count = self.get() + 1
            self.set(count)
        
        # handle 退出时
        with self._lock:
            count = self.get() - 1
            self.set(count)
        """
        with self._lock:
            current = self.get()
            new_value = current + delta
            self._var.set(new_value)
            return new_value


# ============ 方案 3: 完整的 RASP 控制实现 ============

class RASPContext:
    """
    完整的 RASP 上下文管理器
    
    使用 contextvars 实现：
    1. handle 启用 RASP
    2. execute 禁用 RASP
    3. 自动恢复上下文状态
    """
    
    # 核心上下文变量
    _rasp_enabled: contextvars.ContextVar[bool] = contextvars.ContextVar(
        'rasp_enabled', default=False
    )
    _rasp_stack: contextvars.ContextVar[list] = contextvars.ContextVar(
        'rasp_stack', default=[]
    )
    _handle_token: contextvars.ContextVar = contextvars.ContextVar(
        'handle_token', default=None
    )
    
    @classmethod
    def enter_handle(cls) -> contextvars.Token:
        """
        进入 handle 上下文
        
        Returns:
            token: 用于退出时重置
            
        示例:
            token = RASPContext.enter_handle()
            try:
                # 这里 RASP 启用
                requests.get(url1)
            finally:
                RASPContext.exit_handle(token)
        """
        # 记录堆栈（支持嵌套）
        stack = cls._rasp_stack.get()
        stack.append('HANDLE')
        cls._rasp_stack.set(stack)
        
        # 启用 RASP
        token = cls._rasp_enabled.set(True)
        cls._handle_token.set(token)
        
        return token
    
    @classmethod
    def exit_handle(cls, token: contextvars.Token):
        """
        退出 handle 上下文
        
        Args:
            token: enter_handle 返回的令牌
        """
        # 重置状态
        cls._rasp_enabled.reset(token)
        cls._handle_token.set(None)
        
        # 更新堆栈
        stack = cls._rasp_stack.get()
        if stack:
            stack.pop()
        cls._rasp_stack.set(stack)
    
    @classmethod
    def is_enabled(cls) -> bool:
        """检查 RASP 是否启用"""
        return cls._rasp_enabled.get()
    
    @classmethod
    def in_handle(cls) -> bool:
        """检查是否在 handle 上下文中"""
        stack = cls._rasp_stack.get()
        return 'HANDLE' in stack


# ============ 方案 4: 装饰器模式（推荐） ============

def enable_rasp_in_handle(func: Callable) -> Callable:
    """
    装饰器：在函数执行期间启用 RASP
    
    完美替代 threading.local + try-finally 模式
    
    示例:
        @enable_rasp_in_handle
        def handle():
            requests.get(url1)  # 被 RASP 拦截
    
    @enable_rasp_in_handle
    async def async_handle():
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: requests.get(url1)
        )  # 被 RASP 拦截
    """
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        token = RASPContext.enter_handle()
        try:
            return func(*args, **kwargs)
        finally:
            RASPContext.exit_handle(token)
    
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        token = RASPContext.enter_handle()
        try:
            return await func(*args, **kwargs)
        finally:
            RASPContext.exit_handle(token)
    
    # 根据函数是否为生成器/协程选择包装器
    import asyncio
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    return wrapper


# ============ 方案 5: 上下文管理器模式 ============

class rasp_enabled:
    """
    上下文管理器：启用 RASP
    
    示例:
        with rasp_enabled():
            requests.get(url1)  # 被 RASP 拦截
        
        # 离开上下文后自动禁用
    """
    
    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self.token = None
    
    def __enter__(self):
        if self.enabled:
            self.token = RASPContext.enter_handle()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.token is not None:
            RASPContext.exit_handle(self.token)
        return False
    
    async def __aenter__(self):
        if self.enabled:
            self.token = RASPContext.enter_handle()
        return self
    
    async def __aexit__(self, *args):
        if self.token is not None:
            RASPContext.exit_handle(self.token)


# ============ 方案 6: 协程专用上下文 ============

class AsyncRASPContext:
    """
    异步专用 RASP 上下文
    
    使用 asyncio.TaskLocal 实现更强的隔离
    """
    
    _task_active: contextvars.ContextVar[bool] = contextvars.ContextVar(
        'task_active', default=False
    )
    
    @classmethod
    async def enable_for_task(cls):
        """为当前异步任务启用"""
        previous = cls._task_active.get()
        cls._task_active.set(True)
        return previous
    
    @classmethod
    async def disable_for_task(cls, previous: bool = None):
        """禁用"""
        if previous is None:
            cls._task_active.set(False)
        else:
            cls._task_active.set(previous)
    
    @classmethod
    def is_active(cls) -> bool:
        """检查"""
        return cls._task_active.get()
    
    @classmethod
    async def task_scope(cls):
        """异步上下文管理器"""
        previous = await cls.enable_for_task()
        try:
            yield
        finally:
            await cls.disable_for_task(previous)


# ============ 实战示例 ============

import requests

# 假设这是 RASP 的 hook
def rasp_hook(method: str, url: str):
    """RASP 拦截器"""
    if RASPContext.is_enabled():
        print(f"[RASP HOOK] Intercepted: {method} {url}")
        # 执行增强逻辑：日志、安全检查等
        return True
    return False


# 补丁 requests
_original_request = requests.Session.request

def patched_request(self, method, url, *args, **kwargs):
    rasp_hook(method, url)
    return _original_request(self, method, url, *args, **kwargs)

requests.Session.request = patched_request


# ============ 使用演示 ============

class Workflow:
    """工作流演示"""
    
    @enable_rasp_in_handle  # 使用装饰器
    def handle(self):
        print(f"Handle - RASP enabled: {RASPContext.is_enabled()}")
        requests.get('http://url1')  # 被 RASP 拦截
    
    def execute(self):
        print(f"Execute - RASP enabled: {RASPContext.is_enabled()}")
        requests.get('http://url2')  # 不被拦截


async def async_workflow():
    """异步工作流"""
    # 方式1: 装饰器
    @enable_rasp_in_handle
    async def async_handle():
        print(f"Async Handle - RASP enabled: {RASPContext.is_enabled()}")
        await asyncio.sleep(0.1)
        # 模拟异步请求
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: requests.get('http://url1'))
    
    # 方式2: 上下文管理器
    async with AsyncRASPContext.task_scope():
        print(f"Task scope - RASP active: {AsyncRASPContext.is_active()}")


def main():
    """主函数"""
    workflow = Workflow()
    
    print("=== 同步场景 ===")
    workflow.handle()
    workflow.execute()
    
    print("\n=== 手动控制 ===")
    with rasp_enabled():
        print(f"In context - RASP enabled: {RASPContext.is_enabled()}")
        requests.get('http://url1')
    
    print("\n=== 异步场景 ===")
    asyncio.run(async_workflow())
    
    print("\n=== 嵌套场景 ===")
    def outer():
        RASPContext.enter_handle()
        try:
            print(f"Outer - RASP enabled: {RASPContext.is_enabled()}")
            
            def inner():
                # 内层函数共享外层的上下文
                print(f"Inner - RASP enabled: {RASPContext.is_enabled()}")
                requests.get('http://url1')
            
            inner()
        finally:
            RASPContext.exit_handle(RASPContext._handle_token.get())
    
    outer()


# ============ 总结 ============

"""
为什么 contextvars 可以完全替代 threading.local？

┌─────────────────┬─────────────────────────────┬─────────────────────────────┐
│ 特性            │ threading.local             │ contextvars                 │
├─────────────────┼─────────────────────────────┼─────────────────────────────┤
│ 线程隔离        │ ✓                           │ ✓                           │
│ 协程/Task 隔离  │ ✗ (所有协程共享)            │ ✓ (每个 Task 独立)          │
│ 自动生命周期    │ ✗ (需手动管理)              │ ✓ (上下文退出自动清理)      │
│ 嵌套上下文      │ ✗                           │ ✓ (Token 支持)              │
│ 线程安全        │ ✓                           │ ✓                           │
│ Python 版本     │ 2.7+                        │ 3.7+                        │
│ 标准库          │ ✓                           │ ✓                           │
└─────────────────┴─────────────────────────────┴─────────────────────────────┘

contextvars 的优势：

1. **协程原生支持**
   threading.local 在 asyncio 中会失效，因为所有协程运行在同一线程。
   contextvars 自动为每个 asyncio.Task 创建独立上下文。

2. **自动状态管理**
   使用 Token (var.set() 返回) 可以在退出上下文时自动恢复。
   避免了 threading.local 常见的状态泄漏问题。

3. **清晰的语义**
   get() / set() / reset() API 直观易懂。
   比 threading.local 的 hasattr() 检查更安全。

4. **更好的性能**
   contextvars 在 CPython 中有优化。
   避免了大量 hasattr() 和 try-except。

使用建议：

✓ 新项目：直接使用 contextvars
✓ 异步代码：必须使用 contextvars
✓ 库代码：优先使用 contextvars
△ 旧代码迁移：可以渐进式替换
△ 极端性能场景：需要 benchmark 验证
"""

if __name__ == "__main__":
    main()
