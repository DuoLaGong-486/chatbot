"""
FastAPI 异步日志和内存优化解决方案

主要组件：
1. AsyncLogQueue: 异步日志队列（使用线程池，避免阻塞事件循环）
2. BodySizeLimitMiddleware: 请求体大小限制中间件
3. BufferPool: 内存池（复用BytesIO对象）
4. SamplingStrategy: 采样打印策略
5. MemoryMonitor: 内存监控和限流

优势：
- 避免阻塞事件循环
- 限制单个请求的内存使用
- 控制总体内存占用
- 提供灵活的配置选项
"""

import asyncio
import io
import json
import logging
import os
import threading
import time
import tracemalloc
import weakref
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum
from functools import partial
from typing import Any, Callable, Deque, Dict, List, Optional, TypeVar, Union
from queue import Queue, Empty
from threading import Lock

# 配置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
)
logger.addHandler(handler)


# ============ 1. 异步日志队列 ============

class LogPriority(Enum):
    """日志优先级"""
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4


@dataclass(order=True)
class LogMessage:
    """日志消息对象"""
    priority: LogPriority = field(compare=False)
    timestamp: float = field(default_factory=time.time, compare=False)
    message: str = field(compare=False)
    extra: Optional[Dict[str, Any]] = field(default=None, compare=False)
    
    def __post_init__(self):
        # 确保priority是可比较的
        if isinstance(self.priority, int):
            self.priority = LogPriority(self.priority)


class AsyncLogQueue:
    """
    异步日志队列
    
    特点：
    - 使用线程池异步处理日志，避免阻塞事件循环
    - 支持优先级队列
    - 可配置队列大小和丢弃策略
    """
    
    def __init__(
        self,
        max_workers: int = 2,
        max_queue_size: int = 10000,
        drop_policy: str = "oldest",  # "oldest" | "newest" | "none"
        min_level: LogPriority = LogPriority.DEBUG
    ):
        self.max_workers = max_workers
        self.max_queue_size = max_queue_size
        self.drop_policy = drop_policy
        self.min_level = min_level
        
        # 使用线程安全的队列
        self._queue: Queue[LogMessage] = Queue(max_queue_size)
        
        # 线程池
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="AsyncLog"
        )
        
        # 统计信息
        self._stats = {
            "total_logged": 0,
            "dropped": 0,
            "processing_errors": 0
        }
        
        # 启动工作线程
        self._running = True
        self._workers = []
        self._start_workers()
        
        # 使用弱引用管理实例
        weakref.finalize(self, self._shutdown)
    
    def _start_workers(self):
        """启动工作线程"""
        for i in range(self.max_workers):
            worker = threading.Thread(
                target=self._worker_loop,
                name=f"AsyncLogWorker-{i}",
                daemon=True
            )
            worker.start()
            self._workers.append(worker)
    
    def _worker_loop(self):
        """工作线程主循环"""
        while self._running:
            try:
                # 从队列获取日志消息
                log_msg = self._queue.get(timeout=0.1)
                
                # 处理日志消息
                self._process_log(log_msg)
                self._queue.task_done()
                
            except Empty:
                continue
            except Exception as e:
                self._stats["processing_errors"] += 1
                logger.error(f"Error processing log: {e}")
    
    def _process_log(self, log_msg: LogMessage):
        """处理单个日志消息"""
        self._stats["total_logged"] += 1
        
        try:
            # 构建日志输出
            output = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {log_msg.message}"
            
            if log_msg.extra:
                output += f" | Context: {log_msg.extra}"
            
            # 根据优先级选择日志级别
            level = self._priority_to_level(log_msg.priority)
            
            # 实际打印/写入日志
            if level == logging.DEBUG:
                logger.debug(output)
            elif level == logging.INFO:
                logger.info(output)
            elif level == logging.WARNING:
                logger.warning(output)
            elif level == logging.ERROR:
                logger.error(output)
            else:
                logger.critical(output)
                
        except Exception as e:
            self._stats["processing_errors"] += 1
    
    def _priority_to_level(self, priority: LogPriority) -> int:
        """将优先级转换为logging级别"""
        mapping = {
            LogPriority.DEBUG: logging.DEBUG,
            LogPriority.INFO: logging.INFO,
            LogPriority.WARNING: logging.WARNING,
            LogPriority.ERROR: logging.ERROR,
            LogPriority.CRITICAL: logging.CRITICAL
        }
        return mapping.get(priority, logging.INFO)
    
    def put(
        self,
        message: str,
        priority: LogPriority = LogPriority.INFO,
        extra: Optional[Dict[str, Any]] = None,
        block: bool = False
    ):
        """
        添加日志消息到队列
        
        Args:
            message: 日志消息
            priority: 优先级
            extra: 额外上下文信息
            block: 是否阻塞等待
        """
        # 检查优先级
        if priority.value < self.min_level.value:
            return
        
        # 检查队列是否已满
        if self._queue.full():
            if self.drop_policy == "oldest":
                try:
                    # 尝试移除最旧的日志
                    self._queue.get_nowait()
                    self._stats["dropped"] += 1
                except Empty:
                    pass
            elif self.drop_policy == "newest":
                self._stats["dropped"] += 1
                return  # 丢弃新的
            # "none" 策略会阻塞等待（如果block=True）
        
        log_msg = LogMessage(
            priority=priority,
            message=message,
            extra=extra
        )
        
        try:
            if block:
                self._queue.put(log_msg, timeout=1.0)
            else:
                self._queue.put_nowait(log_msg)
        except:
            self._stats["dropped"] += 1
    
    def async_put(self, message: str, extra: Dict[str, Any] = None):
        """
        异步添加日志（不阻塞）
        
        实际上还是添加到队列，但使用非阻塞方式
        """
        self.put(message, priority=LogPriority.INFO, extra=extra, block=False)
    
    def get_stats(self) -> Dict[str, int]:
        """获取统计信息"""
        return {
            "total_logged": self._stats["total_logged"],
            "dropped": self._stats["dropped"],
            "processing_errors": self._stats["processing_errors"],
            "queue_size": self._queue.qsize(),
            "queue_full": self._queue.full()
        }
    
    def flush(self, timeout: float = 5.0):
        """刷新队列中的所有日志"""
        try:
            self._queue.join()
        except:
            pass
    
    def _shutdown(self):
        """关闭队列"""
        self._running = False
        
        # 等待队列清空
        timeout = 5.0
        start = time.time()
        while not self._queue.empty() and (time.time() - start) < timeout:
            time.sleep(0.1)
        
        # 关闭线程池
        self._executor.shutdown(wait=True)
    
    def __del__(self):
        try:
            self._shutdown()
        except:
            pass


# 全局异步日志队列实例
async_log_queue = AsyncLogQueue(
    max_workers=2,
    max_queue_size=5000,
    drop_policy="oldest"
)


# ============ 2. 请求体大小限制中间件 ============

class BodySizeLimitMiddleware:
    """
    请求体大小限制中间件
    
    特点：
    - 在中间件级别限制请求体大小
    - 超过限制则拒绝请求
    - 可配置不同端点的不同限制
    """
    
    def __init__(
        self,
        app,
        max_body_size: int = 1024 * 1024,  # 默认1MB
        default_limit: int = 1024 * 1024,
        path_limits: Dict[str, int] = None
    ):
        self.app = app
        self.max_body_size = max_body_size
        self.default_limit = default_limit
        self.path_limits = path_limits or {}
        
        # 统计信息
        self._stats = {
            "total_requests": 0,
            "rejected_requests": 0,
            "total_bytes_read": 0
        }
    
    async def __call__(self, scope, receive, send):
        """中间件处理逻辑"""
        
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        # 检查路径特定的限制
        path = scope.get("path", "")
        limit = self.path_limits.get(path, self.default_limit)
        
        # 只对POST/PUT/PATCH方法检查body大小
        method = scope.get("method", "")
        if method in ("POST", "PUT", "PATCH"):
            # 读取请求体（带大小限制）
            body = await self._read_body_with_limit(receive, limit)
            
            if body is None:  # 请求被拒绝
                await self._send_error_response(send, 413, "Payload Too Large")
                return
            
            # 将body放入scope供后续使用
            scope["body"] = body
            self._stats["total_bytes_read"] += len(body)
        
        self._stats["total_requests"] += 1
        
        # 继续处理请求
        await self.app(scope, receive, send)
    
    async def _read_body_with_limit(
        self,
        receive,
        limit: int
    ) -> Optional[bytes]:
        """
        读取请求体，带大小限制
        
        Returns:
            bytes: 请求体（如果未超限）
            None: 如果超限
        """
        body_parts = []
        total_size = 0
        
        while True:
            message = await receive()
            
            if message.get("type") == "http.request":
                chunk = message.get("body", b"")
                total_size += len(chunk)
                
                # 检查是否超过限制
                if total_size > limit:
                    # 丢弃请求
                    self._stats["rejected_requests"] += 1
                    async_log_queue.async_put(
                        f"Request body too large: {total_size} > {limit} bytes",
                        extra={"total_size": total_size, "limit": limit}
                    )
                    return None
                
                body_parts.append(chunk)
            
            elif message.get("type") == "http.disconnect":
                break
        
        return b"".join(body_parts)
    
    async def _send_error_response(
        self,
        send,
        status_code: int,
        message: str
    ):
        """发送错误响应"""
        await send({
            "type": "http.response.start",
            "status": status_code,
            "headers": [
                (b"content-type", b"application/json")
            ]
        })
        
        body = json.dumps({
            "error": message,
            "status_code": status_code
        })
        
        await send({
            "type": "http.response.body",
            "body": body.encode()
        })
    
    def get_stats(self) -> Dict[str, int]:
        """获取统计信息"""
        return {
            "total_requests": self._stats["total_requests"],
            "rejected_requests": self._stats["rejected_requests"],
            "total_bytes_read": self._stats["total_bytes_read"]
        }


# ============ 3. 内存池（复用BytesIO对象） ============

class BufferPool:
    """
    BytesIO对象内存池
    
    特点：
    - 复用BytesIO对象，减少内存分配开销
    - 使用弱引用自动清理过期对象
    - 支持对象预创建
    """
    
    def __init__(
        self,
        min_size: int = 1024,
        max_size: int = 1024 * 1024,
        max_idle_objects: int = 100
    ):
        self.min_size = min_size
        self.max_size = max_size
        self.max_idle_objects = max_idle_objects
        
        # 空闲对象池
        self._idle_pool: Deque[io.BytesIO] = deque(maxlen=max_idle_objects)
        self._pool_lock = Lock()
        
        # 使用统计
        self._stats = {
            "created": 0,
            "reused": 0,
            "destroyed": 0,
            "current_size": 0
        }
    
    def acquire(self, initial_size: int = 0) -> io.BytesIO:
        """
        从池中获取一个BytesIO对象
        
        Args:
            initial_size: 初始大小
            
        Returns:
            BytesIO对象
        """
        with self._pool_lock:
            # 尝试从池中获取
            if self._idle_pool:
                buffer = self._idle_pool.pop()
                self._stats["reused"] += 1
                
                # 重置缓冲区
                buffer.seek(0)
                buffer.truncate(0)
            else:
                buffer = io.BytesIO()
                self._stats["created"] += 1
            
            self._stats["current_size"] += 1
            return buffer
    
    def release(self, buffer: io.BytesIO):
        """
        将BytesIO对象归还到池中
        
        Args:
            buffer: BytesIO对象
        """
        if buffer is None:
            return
        
        with self._pool_lock:
            if len(self._idle_pool) < self.max_idle_objects:
                # 重置缓冲区
                try:
                    buffer.seek(0)
                    buffer.truncate(0)
                    self._idle_pool.append(buffer)
                except:
                    self._stats["destroyed"] += 1
            else:
                self._stats["destroyed"] += 1
            
            self._stats["current_size"] -= 1
    
    def trim_pool(self, target_size: int = None):
        """
        缩减池大小
        
        Args:
            target_size: 目标大小
        """
        if target_size is None:
            target_size = self.max_idle_objects // 2
        
        with self._pool_lock:
            while len(self._idle_pool) > target_size:
                try:
                    buffer = self._idle_pool.pop()
                    del buffer
                    self._stats["destroyed"] += 1
                except IndexError:
                    break
    
    def get_stats(self) -> Dict[str, int]:
        """获取统计信息"""
        with self._pool_lock:
            return {
                "created": self._stats["created"],
                "reused": self._stats["reused"],
                "destroyed": self._stats["destroyed"],
                "current_size": self._stats["current_size"],
                "idle_count": len(self._idle_pool)
            }


# 全局缓冲池实例
buffer_pool = BufferPool(
    min_size=1024,
    max_size=1024 * 1024,
    max_idle_objects=50
)


# ============ 4. 采样打印策略 ============

class SamplingStrategy:
    """
    采样打印策略
    
    特点：
    - 按比例采样打印日志
    - 支持不同端点不同采样率
    - 定期调整采样率
    """
    
    def __init__(
        self,
        default_sample_rate: float = 1.0,  # 默认100%打印
        max_samples_per_second: int = 100,
        path_rates: Dict[str, float] = None
    ):
        self.default_sample_rate = default_sample_rate
        self.max_samples_per_second = max_samples_per_second
        self.path_rates = path_rates or {}
        
        # 采样窗口
        self._window_start = time.time()
        self._window_samples = 0
        self._window_lock = Lock()
        
        # 随机数生成器
        self._random = random = __import__('random')
    
    def should_sample(
        self,
        path: str = "",
        operation: str = "log"
    ) -> bool:
        """
        判断是否应该采样
        
        Args:
            path: 请求路径
            operation: 操作类型
            
        Returns:
            bool: 是否应该采样
        """
        # 检查采样窗口限制
        with self._window_lock:
            current_time = time.time()
            
            # 重置窗口（每秒重置一次）
            if current_time - self._window_start >= 1.0:
                self._window_start = current_time
                self._window_samples = 0
            
            # 检查采样窗口限制
            if self._window_samples >= self.max_samples_per_second:
                return False
            
            # 采样窗口计数
            self._window_samples += 1
        
        # 获取路径特定的采样率
        sample_rate = self.path_rates.get(path, self.default_sample_rate)
        
        # 随机采样
        return self._random.random() < sample_rate
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self._window_lock:
            return {
                "samples_this_second": self._window_samples,
                "max_samples_per_second": self.max_samples_per_second,
                "default_sample_rate": self.default_sample_rate,
                "path_rates": self.path_rates
            }


# 全局采样策略实例
sampling_strategy = SamplingStrategy(
    default_sample_rate=1.0,  # 默认100%
    max_samples_per_second=50  # 每秒最多50条
)


# ============ 5. 内存监控和限流 ============

class MemoryMonitor:
    """
    内存监控和限流
    
    特点：
    - 实时监控内存使用
    - 自动触发限流
    - 提供自适应调整
    """
    
    def __init__(
        self,
        memory_limit_mb: int = 512,
        check_interval: float = 1.0,
        auto_throttle_threshold: float = 0.8,  # 80% 开始限流
        max_concurrent_requests: int = 100
    ):
        self.memory_limit_mb = memory_limit_mb
        self.check_interval = check_interval
        self.auto_throttle_threshold = auto_throttle_threshold
        self.max_concurrent_requests = max_concurrent_requests
        
        # 当前状态
        self._current_memory_mb = 0
        self._is_throttled = False
        self._concurrent_requests = 0
        
        # 锁
        self._lock = Lock()
        self._throttle_lock = Lock()
        
        # 启动监控
        self._running = True
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True
        )
        self._monitor_thread.start()
        
        # 统计
        self._stats = {
            "throttle_count": 0,
            "memory_warnings": 0,
            "rejected_requests": 0
        }
    
    def _monitor_loop(self):
        """监控循环"""
        import psutil
        process = psutil.Process()
        
        while self._running:
            try:
                # 获取当前内存使用
                memory_info = process.memory_info()
                self._current_memory_mb = memory_info.rss / (1024 * 1024)
                
                # 检查是否需要限流
                if self._current_memory_mb > self.memory_limit_mb * self.auto_throttle_threshold:
                    self._throttle_lock.acquire()
                    if not self._is_throttled:
                        self._is_throttled = True
                        self._stats["throttle_count"] += 1
                        
                        async_log_queue.async_put(
                            f"Memory throttling ENABLED: {self._current_memory_mb:.2f}MB / {self.memory_limit_mb}MB",
                            extra={"current_mb": self._current_memory_mb, "limit_mb": self.memory_limit_mb}
                        )
                    self._throttle_lock.release()
                
                # 检查内存是否恢复正常
                elif self._current_memory_mb < self.memory_limit_mb * self.auto_throttle_threshold * 0.7:
                    self._throttle_lock.acquire()
                    if self._is_throttled:
                        self._is_throttled = False
                        async_log_queue.async_put(
                            "Memory throttling DISABLED",
                            extra={"current_mb": self._current_memory_mb}
                        )
                    self._throttle_lock.release()
                
                # 内存警告
                if self._current_memory_mb > self.memory_limit_mb * 0.9:
                    self._stats["memory_warnings"] += 1
                    
            except Exception as e:
                logger.error(f"Memory monitor error: {e}")
            
            time.sleep(self.check_interval)
    
    def can_accept_request(self) -> bool:
        """检查是否可以接受新请求"""
        with self._lock:
            if self._is_throttled:
                self._stats["rejected_requests"] += 1
                return False
            
            if self._concurrent_requests >= self.max_concurrent_requests:
                self._stats["rejected_requests"] += 1
                return False
            
            self._concurrent_requests += 1
            return True
    
    def release_request(self):
        """释放请求计数"""
        with self._lock:
            self._concurrent_requests = max(0, self._concurrent_requests - 1)
    
    def get_current_memory(self) -> float:
        """获取当前内存使用（MB）"""
        return self._current_memory_mb
    
    def is_throttled(self) -> bool:
        """是否正在限流"""
        return self._is_throttled
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            **self._stats,
            "current_memory_mb": self._current_memory_mb,
            "memory_limit_mb": self.memory_limit_mb,
            "is_throttled": self._is_throttled,
            "concurrent_requests": self._concurrent_requests,
            "max_concurrent_requests": self.max_concurrent_requests
        }
    
    def shutdown(self):
        """关闭监控"""
        self._running = False
        if self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=2.0)


# ============ 6. ContextVar 请求体捕获 ============

class RequestBodyCapture:
    """
    请求体捕获器
    
    使用contextvar存储请求体，并在请求结束后异步打印
    整合了所有优化策略
    """
    
    def __init__(
        self,
        max_body_size: int = 1024 * 100,  # 100KB用于日志记录
        enable_sampling: bool = True,
        sample_rate: float = 0.1  # 10%的请求会打印完整body
    ):
        self.max_body_size = max_body_size
        self.enable_sampling = enable_sampling
        self.sample_rate = sample_rate
        
        # ContextVar存储请求体
        self._request_body: ContextVar[Optional[bytes]] = ContextVar(
            "request_body", default=None
        )
        self._request_extra: ContextVar[Optional[Dict]] = ContextVar(
            "request_extra", default=None
        )
    
    def capture_request_start(self, body: bytes, extra: Dict = None):
        """
        在请求开始时调用
        
        Args:
            body: 请求体
            extra: 额外信息
        """
        # 截断过大的body
        if len(body) > self.max_body_size:
            body = body[:self.max_body_size] + b"... [truncated]"
        
        self._request_body.set(body)
        self._request_extra.set(extra or {})
    
    def capture_request_end(self):
        """
        在请求结束时调用
        异步打印日志
        """
        body = self._request_body.get()
        extra = self._request_extra.get() or {}
        
        if body is None:
            return
        
        # 采样决定是否打印
        if self.enable_sampling and not sampling_strategy.should_sample():
            return
        
        # 构建日志消息
        try:
            # 尝试解码body
            try:
                body_str = body.decode('utf-8')
                # 进一步截断用于日志
                if len(body_str) > 1000:
                    body_str = body_str[:1000] + "... [truncated]"
                message = f"Request body: {body_str}"
            except:
                message = f"Request body (binary): {len(body)} bytes"
            
            # 异步添加到日志队列
            async_log_queue.async_put(
                message,
                extra=extra
            )
            
        except Exception as e:
            logger.error(f"Error logging request body: {e}")
        
        # 清理contextvar
        self._request_body.set(None)
        self._request_extra.set(None)
    
    def get_captured_body(self) -> Optional[bytes]:
        """获取捕获的请求体"""
        return self._request_body.get()


# 全局请求体捕获器实例
request_body_capture = RequestBodyCapture(
    max_body_size=1024 * 100,  # 100KB
    enable_sampling=True,
    sample_rate=0.1  # 10%
)


# ============ 7. 集成中间件 ============

class OptimizedLoggingMiddleware:
    """
    优化的日志中间件
    
    整合了所有优化策略：
    - 请求体大小限制
    - 异步日志队列
    - 采样打印
    - 内存监控
    """
    
    def __init__(
        self,
        app,
        max_body_size: int = 1024 * 1024,  # 1MB请求体限制
        log_body_size: int = 1024 * 100,  # 100KB用于日志
        enable_sampling: bool = True,
        sample_rate: float = 0.1,
        memory_limit_mb: int = 512,
        max_concurrent_requests: int = 100
    ):
        self.app = app
        
        # 子组件
        self.body_limit_middleware = BodySizeLimitMiddleware(
            app,
            max_body_size=max_body_size
        )
        
        self.capture = RequestBodyCapture(
            max_body_size=log_body_size,
            enable_sampling=enable_sampling,
            sample_rate=sample_rate
        )
        
        self.memory_monitor = MemoryMonitor(
            memory_limit_mb=memory_limit_mb,
            max_concurrent_requests=max_concurrent_requests
        )
    
    async def __call__(self, scope, receive, send):
        """中间件处理"""
        
        # 检查是否可以接受请求
        if not self.memory_monitor.can_accept_request():
            # 返回503 Service Unavailable
            await send({
                "type": "http.response.start",
                "status": 503,
                "headers": [(b"content-type", b"application/json")]
            })
            await send({
                "type": "http.response.body",
                "body": json.dumps({"error": "Service Unavailable - Too many requests"}).encode()
            })
            return
        
        # 获取请求路径和方法
        path = scope.get("path", "")
        method = scope.get("method", "")
        
        # 对于POST请求，读取body
        if method in ("POST", "PUT", "PATCH"):
            try:
                body = await self._read_body(scope, receive)
                
                # 捕获请求体
                self.capture.capture_request_start(body, {
                    "path": path,
                    "method": method
                })
                
                # 将body放入scope
                scope["body"] = body
                scope["received"] = receive
                
            except Exception as e:
                async_log_queue.async_put(
                    f"Error reading body: {e}",
                    extra={"path": path, "method": method}
                )
                await self.app(scope, receive, send)
                return
        else:
            self.capture.capture_request_start(b"", {
                "path": path,
                "method": method
            })
        
        try:
            # 处理请求
            await self.app(scope, receive, send)
        finally:
            # 请求结束，打印日志
            self.capture.capture_request_end()
            # 释放请求计数
            self.memory_monitor.release_request()
    
    async def _read_body(self, scope, receive) -> bytes:
        """读取请求体"""
        body = await receive()
        
        # 如果body是分块的，需要读取所有块
        if body.get("more_body", False):
            body_parts = [body.get("body", b"")]
            
            while True:
                chunk = await receive()
                body_parts.append(chunk.get("body", b""))
                
                if not chunk.get("more_body", False):
                    break
            
            return b"".join(body_parts)
        
        return body.get("body", b"")
    
    def get_all_stats(self) -> Dict[str, Any]:
        """获取所有统计信息"""
        return {
            "body_limit": self.body_limit_middleware.get_stats(),
            "memory": self.memory_monitor.get_stats(),
            "sampling": sampling_strategy.get_stats(),
            "buffer_pool": buffer_pool.get_stats(),
            "log_queue": async_log_queue.get_stats()
        }


# ============ 8. 便捷工厂函数 ============

def create_optimized_middleware(
    app,
    max_body_size_mb: float = 1.0,
    log_body_size_kb: float = 100.0,
    sample_rate: float = 0.1,
    memory_limit_mb: int = 512,
    max_concurrent_requests: int = 100,
    log_workers: int = 2
) -> OptimizedLoggingMiddleware:
    """
    创建优化的日志中间件
    
    Args:
        app: FastAPI应用
        max_body_size_mb: 最大请求体大小（MB）
        log_body_size_kb: 用于日志记录的请求体大小（KB）
        sample_rate: 采样率
        memory_limit_mb: 内存限制（MB）
        max_concurrent_requests: 最大并发请求数
        log_workers: 日志工作线程数
        
    Returns:
        OptimizedLoggingMiddleware实例
    """
    # 更新全局日志队列配置
    global async_log_queue
    async_log_queue = AsyncLogQueue(
        max_workers=log_workers,
        max_queue_size=5000,
        drop_policy="oldest"
    )
    
    return OptimizedLoggingMiddleware(
        app=app,
        max_body_size=int(max_body_size_mb * 1024 * 1024),
        log_body_size=int(log_body_size_kb * 1024),
        enable_sampling=True,
        sample_rate=sample_rate,
        memory_limit_mb=memory_limit_mb,
        max_concurrent_requests=max_concurrent_requests
    )


# ============ 9. FastAPI集成 ============

def setup_fastapi_app(app) -> OptimizedLoggingMiddleware:
    """
    设置FastAPI应用的优化中间件
    
    Args:
        app: FastAPI应用实例
        
    Returns:
        配置好的OptimizedLoggingMiddleware实例
    """
    middleware = create_optimized_middleware(app)
    app.add_middleware(OptimizedLoggingMiddleware)
    
    # 添加健康检查端点
    @app.get("/_optimization_stats")
    async def optimization_stats():
        """获取优化统计信息"""
        return middleware.get_all_stats()
    
    @app.get("/_health")
    async def health_check():
        """健康检查"""
        return {
            "status": "ok",
            "memory_mb": middleware.memory_monitor.get_current_memory()
        }
    
    return middleware


# ============ 10. 使用示例 ============

if __name__ == "__main__":
    """
    使用示例
    """
    from fastapi import FastAPI, Request
    from pydantic import BaseModel
    
    app = FastAPI()
    
    # 创建优化中间件
    middleware = create_optimized_middleware(
        app,
        max_body_size_mb=2.0,
        log_body_size_kb=50.0,
        sample_rate=0.2,
        memory_limit_mb=256,
        max_concurrent_requests=50
    )
    
    # 方式1：直接添加到app（注意：FastAPI需要不同的方式）
    # 推荐在main.py中直接使用
    
    class Item(BaseModel):
        name: str
        description: str = None
    
    @app.post("/items/")
    async def create_item(item: Item):
        """示例端点"""
        return {"name": item.name, "description": item.description}
    
    @app.get("/test/")
    async def test_endpoint():
        """测试端点"""
        return {"message": "Hello World"}
    
    # 启动服务器
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
