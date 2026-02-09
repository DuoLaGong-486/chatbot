"""
SSE Streaming Example - Client端
使用 requests 库处理 Server-Sent Events (SSE) 流式响应

功能:
1. 基础 SSE 接收 - 处理流式文本数据
2. JSON 数据解析 - 解析 SSE 中的 JSON 数据
3. 事件类型处理 - 根据 event 字段分发处理
4. 错误处理和重连机制
5. 多种应用场景示例

安装依赖:
    pip install -r requirements.txt

运行示例:
    python client.py

注意: 运行前请先启动服务器 (python server.py)
"""

import json
import re
import sys
import time
import threading
from typing import Callable, Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum
import requests


class SSECLientState(Enum):
    """SSE 客户端状态"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


@dataclass
class SSEEvent:
    """SSE 事件数据类"""
    event: str
    data: str | dict
    id: Optional[str]
    retry: Optional[int]
    raw_line: str

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            "event": self.event,
            "data": self.data,
            "id": self.id,
            "retry": self.retry
        }


class SSEParseError(Exception):
    """SSE 解析错误"""
    pass


class SSEClient:
    """
    SSE (Server-Sent Events) 客户端

    使用 requests 库接收和处理 SSE 流
    """

    def __init__(
            self,
            url: str,
            event_handler: Callable[[SSEEvent], None] = None,
            error_handler: Callable[[Exception], None] = None,
            connection_error_handler: Callable[[], None] = None,
            max_retry: int = 3,
            retry_delay: float = 1.0,
            headers: Optional[Dict[str, str]] = None,
            timeout: float = 30.0,
            **kwargs
    ):
        """
        初始化 SSE 客户端

        Args:
            url: SSE 服务器 URL
            event_handler: 事件处理回调函数
            error_handler: 错误处理回调函数
            connection_error_handler: 连接错误处理回调函数
            max_retry: 最大重试次数
            retry_delay: 重试延迟 (秒)
            headers: 请求头
            timeout: 请求超时时间 (秒)
            **kwargs: 传递给 requests 的其他参数
        """
        self.url = url
        self.event_handler = event_handler
        self.error_handler = error_handler
        self.connection_error_handler = connection_error_handler
        self.max_retry = max_retry
        self.retry_delay = retry_delay
        self.headers = headers or {}
        self.timeout = timeout
        self.kwargs = kwargs

        self.state = SSECLientState.DISCONNECTED
        self._response = None
        self._session = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._event_counts: Dict[str, int] = {}

    def _create_session(self) -> requests.Session:
        """创建请求会话"""
        session = requests.Session()
        # 设置默认请求头
        session.headers.update({
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            **self.headers
        })
        return session

    def _parse_sse_line(self, line: str) -> tuple:
        """
        解析单行 SSE 数据

        Args:
            line: 原始行数据

        Returns:
            (field, value) 元组
        """
        line = line.rstrip("\r")

        if not line:
            return None, None

        # 处理注释行
        if line.startswith(":"):
            return ":", line[1:]

        # 解析 field: value
        if ": " in line:
            field, value = line.split(": ", 1)
            return field.lower(), value
        elif ":" in line:
            field, value = line.split(":", 1)
            return field.lower(), value

        return None, None

    def _parse_sse_data(self, data_str: str) -> str | dict:
        """
        解析 SSE 数据字段

        Args:
            data_str: 数据字符串

        Returns:
            解析后的数据 (字符串或字典)
        """
        # 尝试解析为 JSON
        try:
            return json.loads(data_str)
        except json.JSONDecodeError:
            return data_str

    def _parse_sse_event(self, lines: List[str]) -> SSEEvent:
        """
        解析完整的 SSE 事件

        Args:
            lines: 事件的所有行

        Returns:
            SSEEvent 对象

        Raises:
            SSEParseError: 解析失败
        """
        event_type = "message"
        data = ""
        event_id = None
        retry = None

        for line in lines:
            if not line.strip():
                continue

            field, value = self._parse_sse_line(line)

            if field is None:
                continue

            if field == "event":
                event_type = value
            elif field == "data":
                data += value + "\n"
            elif field == "id":
                event_id = value
            elif field == "retry":
                try:
                    retry = int(value)
                except ValueError:
                    pass

        # 移除末尾的换行符
        data = data.rstrip("\n")

        # 解析数据
        parsed_data = self._parse_sse_data(data)

        # 合并原始行用于调试
        raw_line = "\n".join(lines)

        return SSEEvent(
            event=event_type,
            data=parsed_data,
            id=event_id,
            retry=retry,
            raw_line=raw_line
        )

    def _process_stream(self, response: requests.Response):
        """
        处理流式响应

        Args:
            response: requests.Response 对象
        """
        buffer = ""

        # 启用原始流解码
        # 使用 raw.stream() 直接读取底层 socket 数据
        try:
            # 解码器用于处理 gzip/chunked 编码
            decoder = response.raw

            while True:
                # 使用 raw.stream() 读取数据块
                chunk = decoder.read(chunk_size=1024, decode_unicode=True)
                if not chunk:
                    break

                buffer += chunk

                # 按行处理
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.rstrip("\r")

                    # 空行表示事件结束
                    if not line.strip():
                        if buffer.strip():
                            try:
                                event = self._parse_sse_event(buffer.split("\n"))
                                self._handle_event(event)
                            except Exception as e:
                                if self.error_handler:
                                    self.error_handler(e)
                        buffer = ""
                        continue

                    buffer += line + "\n"

        except Exception as e:
            if self.error_handler:
                self.error_handler(e)
            raise

    def _handle_event(self, event: SSEEvent):
        """
        处理解析后的事件

        Args:
            event: SSEEvent 对象
        """
        # 更新事件计数
        self._event_counts[event.event] = self._event_counts.get(event.event, 0) + 1

        # 调用事件处理回调
        if self.event_handler:
            self.event_handler(event)

    def _connect(self):
        """建立连接"""
        self.state = SSECLientState.CONNECTING

        # 创建会话
        if self._session is None:
            self._session = self._create_session()

        try:
            # 发起 GET 请求，设置 stream=True
            self._response = self._session.get(
                self.url,
                stream=True,
                timeout=self.timeout,
                **self.kwargs
            )
            self._response.raise_for_status()


            self.state = SSECLientState.CONNECTED
            self._event_counts.clear()

            # 处理流
            self._process_stream(self._response)

        except requests.exceptions.ConnectionError as e:
            self.state = SSECLientState.ERROR
            if self.connection_error_handler:
                self.connection_error_handler()
            raise e

        except requests.exceptions.RequestException as e:
            self.state = SSECLientState.ERROR
            if self.error_handler:
                self.error_handler(e)
            raise e

    def start(self, blocking: bool = True):
        """
        启动 SSE 客户端

        Args:
            blocking: 是否阻塞模式运行
        """
        self._running = True

        if blocking:
            self._run_with_retry()
        else:
            # 在单独线程中运行
            self._thread = threading.Thread(target=self._run_with_retry, daemon=True)
            self._thread.start()

    def _run_with_retry(self):
        """带重试的运行"""
        retry_count = 0

        while self._running and retry_count < self.max_retry:
            try:
                self._connect()
            except Exception as e:
                retry_count += 1

                if self._running and retry_count < self.max_retry:
                    self.state = SSECLientState.RECONNECTING
                    print(f"连接失败，{self.retry_delay}秒后重试... ({retry_count}/{self.max_retry})")
                    time.sleep(self.retry_delay)
                else:
                    print(f"已达到最大重试次数，放弃连接")
                    break

    def stop(self):
        """停止 SSE 客户端"""
        self._running = False

        if self._response:
            self._response.close()

        if self._session:
            self._session.close()

        self.state = SSECLientState.DISCONNECTED

    @property
    def is_connected(self) -> bool:
        """检查是否已连接"""
        return self.state == SSECLientState.CONNECTED

    @property
    def event_counts(self) -> Dict[str, int]:
        """获取事件计数"""
        return self._event_counts.copy()


# ==================== 便捷函数 ====================

def connect_sse(
        url: str,
        on_event: Callable[[SSEEvent], None],
        on_error: Callable[[Exception], None] = None,
        timeout: float = 30.0
) -> SSEClient:
    """
    便捷的 SSE 连接函数

    Args:
        url: SSE 服务器 URL
        on_event: 事件处理回调
        on_error: 错误处理回调
        timeout: 超时时间

    Returns:
        SSEClient 实例
    """
    client = SSEClient(
        url=url,
        event_handler=on_event,
        error_handler=on_error,
        timeout=timeout
    )
    client.start()
    return client


# ==================== 使用示例 ====================

def example_basic_usage():
    """基础使用示例"""
    print("\n" + "=" * 50)
    print("示例 1: 基础 SSE 接收")
    print("=" * 50)

    def handle_event(event: SSEEvent):
        print(f"收到事件: type={event.event}, data={event.data}")

    client = SSEClient(
        url="http://127.0.0.1:8000/stream/time",
        event_handler=handle_event,
        max_retry=1
    )

    try:
        client.start(blocking=True)
    except KeyboardInterrupt:
        print("\n用户中断，正在停止客户端...")
        client.stop()


def example_stock_stream():
    """股票数据流示例"""
    print("\n" + "=" * 50)
    print("示例 2: 股票数据流接收")
    print("=" * 50)

    event_counts = {}

    def handle_event(event: SSEEvent):
        if event.event == "stock_update":
            data = event.data
            print(
                f"股票: {data['stock_code']} | "
                f"价格: ${data['price']:.2f} | "
                f"涨跌: {data['change']:+.2f} | "
                f"成交量: {data['volume']:,}"
            )
        elif event.event == "error":
            print(f"错误: {event.data}")

    client = SSEClient(
        url="http://127.0.0.1:8000/stream/stocks",
        event_handler=handle_event,
        max_retry=1
    )

    try:
        print("连接股票数据流 (5秒后自动停止)...")
        client.start(blocking=False)
        time.sleep(5)
        client.stop()
        print(f"\n共接收事件: {client.event_counts}")
    except KeyboardInterrupt:
        client.stop()


def example_task_progress():
    """任务进度示例"""
    print("\n" + "=" * 50)
    print("示例 3: 任务进度流接收")
    print("=" * 50)

    task_id = f"task-{int(time.time())}"

    def handle_event(event: SSEEvent):
        if event.event == "progress":
            data = event.data
            progress_bar = "█" * (data['progress'] // 10) + "░" * (10 - data['progress'] // 10)
            print(
                f"\r[{progress_bar}] {data['progress']:3d}% | "
                f"{data['current_step']:<20} | "
                f"预计剩余: {data['estimated_time_remaining']}秒",
                end="", flush=True
            )
        elif event.event == "retry":
            print(f"\n服务端建议重连时间: {event.retry}ms")

    client = SSEClient(
        url=f"http://127.0.0.1:8000/stream/progress/{task_id}",
        event_handler=handle_event,
        max_retry=1
    )

    try:
        print(f"任务ID: {task_id}")
        client.start(blocking=True)
    except KeyboardInterrupt:
        client.stop()


def example_chat_messages():
    """聊天消息示例"""
    print("\n" + "=" * 50)
    print("示例 4: 聊天消息流接收")
    print("=" * 50)

    def handle_event(event: SSEEvent):
        if event.event == "chat_message":
            data = event.data
            msg_type = data.get("type", "user")

            if msg_type == "system":
                print(f"🔔 系统: {data['content']}")
            else:
                print(f"👤 {data['user']}: {data['content']}")

    client = SSEClient(
        url="http://127.0.0.1:8000/stream/chat",
        event_handler=handle_event,
        max_retry=1
    )

    try:
        print("连接聊天流 (15秒后自动停止)...")
        client.start(blocking=False)
        time.sleep(15)
        client.stop()
        print(f"\n共接收消息: {client.event_counts}")
    except KeyboardInterrupt:
        client.stop()


def example_heartbeat():
    """心跳检测示例"""
    print("\n" + "=" * 50)
    print("示例 5: 心跳检测")
    print("=" * 50)

    last_heartbeat_time = None
    heartbeat_timeouts = []

    def handle_event(event: SSEEvent):
        nonlocal last_heartbeat_time

        if event.event == "heartbeat":
            current_time = time.time()

            if last_heartbeat_time:
                interval = current_time - last_heartbeat_time
                heartbeat_timeouts.append(interval)

            last_heartbeat_time = current_time
            data = event.data

            print(
                f"💓 心跳 | "
                f"服务器状态: {data['server_status']} | "
                f"时间戳: {data['timestamp']}"
            )

    client = SSEClient(
        url="http://127.0.0.1:8000/stream/heartbeat",
        event_handler=handle_event,
        max_retry=1
    )

    try:
        print("连接心跳流 (10秒后自动停止)...")
        client.start(blocking=False)
        time.sleep(10)
        client.stop()

        if heartbeat_timeouts:
            avg_interval = sum(heartbeat_timeouts) / len(heartbeat_timeouts)
            print(f"\n平均心跳间隔: {avg_interval:.2f}秒")
    except KeyboardInterrupt:
        client.stop()


def example_error_handling():
    """错误处理示例"""
    print("\n" + "=" * 50)
    print("示例 6: 错误处理")
    print("=" * 50)

    def handle_error(error: Exception):
        print(f"❌ 错误发生: {type(error).__name__}: {error}")

    # 测试连接不存在的端点
    client = SSEClient(
        url="http://127.0.0.1:8000/stream/nonexistent",
        error_handler=handle_error,
        max_retry=1
    )

    try:
        print("尝试连接不存在的端点...")
        client.start(blocking=True)
    except Exception as e:
        print(f"连接失败: {e}")


def example_context_manager():
    """上下文管理器示例"""
    print("\n" + "=" * 50)
    print("示例 7: 上下文管理器方式")
    print("=" * 50)

    # 使用上下文管理器自动管理连接
    event_count = [0]

    def handle_event(event: SSEEvent):
        event_count[0] += 1
        if event_count[0] <= 5:
            print(f"事件 {event_count[0]}: {event.event} - {event.data}")

    # 注意: requests 不支持直接的上下文管理器
    # 这里演示手动管理方式
    client = SSEClient(
        url="http://127.0.0.1:8000/stream/time",
        event_handler=handle_event,
        max_retry=1
    )

    try:
        client.start(blocking=False)
        print("开始接收事件 (3秒后停止)...")
        time.sleep(3)
    finally:
        client.stop()
        print(f"总共接收 {event_count[0]} 个事件")


def example_with_requests_session():
    """直接使用 requests 会话示例"""
    print("\n" + "=" * 50)
    print("示例 8: 直接使用 requests 接收 SSE")
    print("=" * 50)

    url = "http://127.0.0.1:8000/stream/time"

    # 创建会话
    session = requests.Session()
    session.headers.update({
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache"
    })

    try:
        response = session.get(url, stream=True, timeout=30)
        response.raise_for_status()


        print("直接使用 requests.raw.stream() 接收 SSE 流:")

        buffer = b""
        event_count = 0

        # 使用 raw.stream() 读取数据
        for chunk in response.raw.stream(1024, decode_content=True):
            buffer += chunk

            print("当前缓冲区:", buffer)

    except Exception as e:
        print(f"错误: {e}")
    finally:
        session.close()


import functools
from urllib3.response import HTTPResponse

original_stream = HTTPResponse.stream

@functools.wraps(original_stream)
def patched_stream(self, amt=2**16, decode_content=None):
    for data in original_stream(self, amt, decode_content):
        print(f"[STREAM DATA] {len(data)} bytes: {data}")
        yield data
    # self.close()

HTTPResponse.stream = patched_stream

# ==================== 主函数 ====================

def main():
    """主函数 - 运行示例"""
    print("=" * 60)
    print("SSE Streaming Client 示例")
    print("=" * 60)
    print("\n确保服务器已启动: python server.py")
    print("\n请选择要运行的示例:")
    print("  1. 基础 SSE 接收 (时间流)")
    print("  2. 股票数据流")
    print("  3. 任务进度流")
    print("  4. 聊天消息流")
    print("  5. 心跳检测")
    print("  6. 错误处理")
    print("  7. 上下文管理器方式")
    print("  8. 直接使用 requests")
    print("  a. 运行所有示例")
    print("  q. 退出")

    choice = input("\n请输入选项: ").strip().lower()

    examples = {
        "1": example_basic_usage,
        "2": example_stock_stream,
        "3": example_task_progress,
        "4": example_chat_messages,
        "5": example_heartbeat,
        "6": example_error_handling,
        "7": example_context_manager,
        "8": example_with_requests_session,
    }

    if choice == "q":
        print("退出程序")
        sys.exit(0)
    elif choice == "a":
        # 运行所有示例
        for i, example in enumerate(examples.values(), 1):
            try:
                example()
                time.sleep(1)  # 示例间隔
            except KeyboardInterrupt:
                print("\n用户中断")
                break
            except Exception as e:
                print(f"示例运行出错: {e}")
                time.sleep(1)
    elif choice in examples:
        try:
            examples[choice]()
        except KeyboardInterrupt:
            print("\n用户中断")
        except Exception as e:
            print(f"错误: {e}")
    else:
        print("无效选项")

    print("\n程序结束")


if __name__ == "__main__":
    main()
