"""
RASP ContextVar Flag 方案
=========================

核心流程：
1. handle 的 before 阶段：设置 rasp_enabled = True
2. requests 拦截时：判断 flag 是否为 True
3. handle 的 after 阶段：设置 rasp_enabled = False

特点：
- 简洁直观
- 自动状态恢复（使用 Token）
- 完美支持线程/协程
"""

import contextvars
import asyncio
from functools import wraps
from typing import Callable, Optional
import requests


# ============ 核心：RASP 标志位管理 ============

class RASPFlag:
    """
    基于 contextvar 的 RASP 标志位管理
    
    API:
    - enable(): before 阶段启用
    - disable(): after 阶段禁用
    - is_enabled(): requests 判断
    """
    
    # 核心标志位 - default=False 表示默认不启用
    _enabled: contextvars.ContextVar[bool] = contextvars.ContextVar(
        'rasp_flag', default=False
    )
    
    @classmethod
    def enable(cls) -> contextvars.Token:
        """
        【Before】启用 RASP
        
        Returns:
            token: 用于 after 阶段恢复状态
            
        示例:
            token = RASPFlag.enable()
            try:
                requests.get(url1)  # 被拦截
            finally:
                RASPFlag.disable(token)
        """
        # 设置为 True，返回 token 用于后续恢复
        return cls._enabled.set(True)
    
    @classmethod
    def disable(cls, token: Optional[contextvars.Token] = None):
        """
        【After】禁用 RASP
        
        Args:
            token: enable() 返回的令牌。如果为 None，直接设为 False
            
        示例:
            RASPFlag.disable()  # 简单方式
            # 或
            token = RASPFlag.enable()
            ...
            RASPFlag.disable(token)  # 安全方式（支持嵌套）
        """
        if token is None:
            # 简单方式：直接设为 False
            cls._enabled.set(False)
        else:
            # 安全方式：恢复到之前的状态
            cls._enabled.reset(token)
    
    @classmethod
    def is_enabled(cls) -> bool:
        """
        【判断】检查 RASP 是否启用
        
        Returns:
            True: 执行增强逻辑
            False: 跳过增强逻辑
            
        示例:
            if RASPFlag.is_enabled():
                print("执行 RASP 增强逻辑")
        """
        return cls._enabled.get()
    
    @classmethod
    def context(cls):
        """
        上下文管理器方式
        
        示例:
            with RASPFlag.context():
                requests.get(url1)  # 被拦截
        """
        return _RASPContextManager()


class _RASPContextManager:
    """RASP 上下文管理器"""
    
    def __enter__(self):
        self._token = RASPFlag.enable()
        return self
    
    def __exit__(self, *args):
        RASPFlag.disable(self._token)
    
    async def __aenter__(self):
        self._token = RASPFlag.enable()
        return self
    
    async def __aexit__(self, *args):
        RASPFlag.disable(self._token)


# ============ 装饰器：自动化 before/after ============

def rasp_scope(func: Callable) -> Callable:
    """
    装饰器：自动管理 RASP 开关
    
    适用于 handle 函数
    
    示例:
        @rasp_scope
        def handle():
            requests.get(url1)  # 被拦截
    """
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        token = RASPFlag.enable()
        try:
            return func(*args, **kwargs)
        finally:
            RASPFlag.disable(token)
    
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        token = RASPFlag.enable()
        try:
            return await func(*args, **kwargs)
        finally:
            RASPFlag.disable(token)
    
    # 根据函数类型选择包装器
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    return wrapper


# ============ RASP Hook 实现 ============

class RASPHook:
    """
    RASP 拦截器
    
    工作流程：
    1. requests 发送前 → hook 拦截
    2. 判断 RASPFlag.is_enabled()
    3. 决定是否执行增强逻辑
    """
    
    _original_request = None
    
    @classmethod
    def install(cls):
        """安装 hook"""
        if cls._original_request is not None:
            return  # 已安装
        
        cls._original_request = requests.Session.request
        
        def patched(self, method, url, *args, **kwargs):
            # 【判断】检查是否启用 RASP
            if RASPFlag.is_enabled():
                cls._enhance(method, url, *args, **kwargs)
            
            # 调用原始方法
            return cls._original_request(self, method, url, *args, **kwargs)
        
        requests.Session.request = patched
    
    @classmethod
    def uninstall(cls):
        """卸载 hook"""
        if cls._original_request is not None:
            requests.Session.request = cls._original_request
            cls._original_request = None
    
    @classmethod
    def _enhance(cls, method: str, url: str, *args, **kwargs):
        """
        增强逻辑
        
        这里可以添加：
        - 日志记录
        - 安全检查
        - 参数验证
        - 限流控制
        """
        print(f"[RASP HOOK] 🔒 拦截请求: {method} {url}")
        # ... 执行增强逻辑


# ============ 使用示例 ============

# 安装 RASP Hook
RASPHook.install()


class Workflow:
    """工作流示例"""
    
    @rasp_scope  # 自动管理 before/after
    def handle(self):
        """
        处理阶段 - RASP 已启用
        
        进入时：RASPFlag.enable()
        退出时：RASPFlag.disable()
        """
        print(f"[Handle] RASP enabled: {RASPFlag.is_enabled()}")
        
        # 发送请求 - 会被 RASP 拦截
        resp1 = requests.get('http://url1')
        print(f"[Handle] Response 1: {resp1.status_code}")
        
        # 嵌套调用 - 仍然在 handle 上下文中
        self._nested_request()
        
        resp2 = requests.get('http://url1')
        print(f"[Handle] Response 2: {resp2.status_code}")
    
    def _nested_request(self):
        """嵌套请求 - 共享上下文"""
        print(f"[Nested] RASP enabled: {RASPFlag.is_enabled()}")
        requests.get('http://url1')  # 仍然被拦截
    
    def execute(self):
        """
        执行阶段 - RASP 已禁用
        """
        print(f"[Execute] RASP enabled: {RASPFlag.is_enabled()}")
        
        # 发送请求 - 不会被 RASP 拦截
        resp = requests.get('http://url2')
        print(f"[Execute] Response: {resp.status_code}")


def manual_demo():
    """手动控制示例"""
    
    print("=== 方式 1: 手动控制 ===")
    
    # Before
    token = RASPFlag.enable()
    try:
        print(f"[Manual] RASP enabled: {RASPFlag.is_enabled()}")
        requests.get('http://url1')  # 被拦截
    finally:
        # After
        RASPFlag.disable(token)
    
    print(f"[After] RASP enabled: {RASPFlag.is_enabled()}")
    requests.get('http://url2')  # 不被拦截
    
    print("\n=== 方式 2: 上下文管理器 ===")
    with RASPFlag.context():
        print(f"[Context] RASP enabled: {RASPFlag.is_enabled()}")
        requests.get('http://url1')  # 被拦截
    
    print(f"[Exit] RASP enabled: {RASPFlag.is_enabled()}")
    requests.get('http://url2')  # 不被拦截


async def async_demo():
    """异步示例"""
    
    print("\n=== 异步场景 ===")
    
    @rasp_scope
    async def async_handle():
        print(f"[Async] RASP enabled: {RASPFlag.is_enabled()}")
        
        # 模拟异步操作
        await asyncio.sleep(0.1)
        
        # 在线程池中执行请求
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: requests.get('http://url1'))
        
        print(f"[Async] Done")
    
    await async_handle()


async def concurrent_demo():
    """并发示例 - 展示协程隔离"""
    
    print("\n=== 并发协程隔离测试 ===")
    
    async def task(name: str, enable_flag: bool):
        if enable_flag:
            token = RASPFlag.enable()
        
        try:
            print(f"[{name}] RASP enabled: {RASPFlag.is_enabled()}")
            # 模拟并发请求
            await asyncio.sleep(0.1)
            requests.get(f'http://{name}.api')
        finally:
            if enable_flag:
                RASPFlag.disable(token)
    
    # 并发执行：task1 启用 RASP，task2 不启用
    await asyncio.gather(
        task("task1", enable_flag=True),
        task("task2", enable_flag=False)
    )


def main():
    workflow = Workflow()
    
    print("=" * 50)
    print("【Handle】RASP 启用")
    print("=" * 50)
    workflow.handle()
    
    print("\n" + "=" * 50)
    print("【Execute】RASP 禁用")
    print("=" * 50)
    workflow.execute()
    
    manual_demo()
    asyncio.run(async_demo())
    asyncio.run(concurrent_demo())


if __name__ == "__main__":
    main()
