"""
SSE Streaming Example - Server端
使用 FastAPI 实现 Server-Sent Events (SSE) 服务器

功能:
1. 基础 SSE 端点 - 简单的文本流
2. 实时数据流 - 模拟股票行情/传感器数据
3. 进度通知 - 模拟长时间任务进度
4. 聊天消息流 - 模拟实时聊天
5. 心跳检测 - 保持连接活跃

安装依赖:
    pip install -r requirements.txt

运行服务器:
    python server.py

默认监听: http://127.0.0.1:8000
"""
from core import HTTP_CTX
import asyncio
import json
import random
import time
import uuid
from datetime import datetime
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# 创建 FastAPI 应用
app = FastAPI(
    title="SSE Streaming Server",
    description="Server-Sent Events 示例服务器",
    version="1.0.0"
)

# 添加 CORS 中间配置，允许跨域请求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== 数据模型 ====================

class Message(BaseModel):
    """聊天消息模型"""
    user_id: str
    username: str
    content: str
    timestamp: datetime = None


class TaskProgress(BaseModel):
    """任务进度模型"""
    task_id: str
    progress: int  # 0-100
    status: str
    current_step: str
    estimated_time_remaining: int  # 秒


# ==================== SSE 工具函数 ====================

def format_sse_event(
        data: dict | str,
        event: str = "message",
        id: str = None,
        retry: int = None
) -> str:
    """
    格式化 SSE 事件数据

    SSE 格式:
    event: <event_type>
    id: <unique_id>
    retry: <reconnect_time_ms>
    data: <json_string_or_text>

    Args:
        data: 要发送的数据 (dict 会自动转为 JSON)
        event: 事件类型
        id: 事件 ID
        retry: 重连时间 (毫秒)

    Returns:
        格式化的 SSE 数据字符串
    """
    lines = []

    if event:
        lines.append(f"event: {event}")

    if id:
        lines.append(f"id: {id}")

    if retry:
        lines.append(f"retry: {retry}")

    # 处理数据
    if isinstance(data, dict):
        json_data = json.dumps(data, ensure_ascii=False)
        lines.append(f"data: {json_data}")
    else:
        lines.append(f"data: {data}")

    # SSE 数据以空行结束
    lines.append("")
    return "\n".join(lines)


async def async_format_sse(
        data: dict | str,
        event: str = "message",
        id: str = None,
        retry: int = None
) -> str:
    """异步格式化 SSE 事件"""
    return format_sse_event(data, event, id, retry)


# ==================== SSE 端点实现 ====================

async def generate_time_stream() -> AsyncGenerator[str, None]:
    """
    生成时间流 - 每秒发送当前时间

    这是一个最简单的 SSE 示例，展示基本概念
    """
    for i in range(5):  # 发送60秒
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sse_data = format_sse_event(
            data={
                "timestamp": current_time,
                "counter": i,
                "message": f"服务器时间: {current_time}"
            },
            event="time_update",
            id=str(uuid.uuid4())
        )

        yield sse_data
        await asyncio.sleep(1)  # 模拟异步IO


async def generate_stock_stream() -> AsyncGenerator[str, None]:
    """
    生成模拟股票数据流

    模拟实时股票行情推送，展示高频数据更新
    """
    stock_codes = ["AAPL", "GOOGL", "MSFT", "AMZN", "META"]

    for i in range(100):
        stock_code = random.choice(stock_codes)
        base_price = {
            "AAPL": 175.0,
            "GOOGL": 140.0,
            "MSFT": 378.0,
            "AMZN": 178.0,
            "META": 505.0
        }[stock_code]

        # 生成随机价格波动
        change = random.uniform(-2.0, 2.0)
        current_price = round(base_price + change, 2)

        sse_data = format_sse_event(
            data={
                "stock_code": stock_code,
                "price": current_price,
                "change": round(change, 2),
                "timestamp": datetime.now().isoformat(),
                "volume": random.randint(10000, 1000000)
            },
            event="stock_update",
            id=f"stock-{i}"
        )

        yield sse_data
        await asyncio.sleep(0.5)  # 每500ms更新一次


async def generate_task_progress(task_id: str) -> AsyncGenerator[str, None]:
    """
    生成任务进度流

    模拟长时间运行任务的进度通知
    """
    steps = [
        "初始化任务...",
        "加载配置...",
        "验证输入参数...",
        "正在处理数据 (1/5)...",
        "正在处理数据 (2/5)...",
        "正在处理数据 (3/5)...",
        "正在处理数据 (4/5)...",
        "正在处理数据 (5/5)...",
        "保存结果...",
        "清理临时文件...",
        "任务完成!"
    ]

    for i, step in enumerate(steps):
        progress = int((i + 1) / len(steps) * 100)

        task_data = {
            "task_id": task_id,
            "progress": progress,
            "status": "running" if i < len(steps) - 1 else "completed",
            "current_step": step,
            "estimated_time_remaining": max(0, len(steps) - i - 1) * 2
        }

        sse_data = format_sse_event(
            data=task_data,
            event="progress",
            id=f"progress-{i}"
        )

        yield sse_data

        # 模拟处理时间
        await asyncio.sleep(random.uniform(0.5, 1.5))


async def generate_chat_stream(request: Request) -> AsyncGenerator[str, None]:
    """
    生成聊天消息流

    模拟实时聊天系统，支持广播消息
    """
    # 模拟一些初始消息
    welcome_messages = [
        {"user": "系统", "content": "欢迎来到实时聊天!", "type": "system"},
        {"user": "Alice", "content": "大家好!", "type": "user"},
        {"user": "Bob", "content": "Hi, 很高兴来到这里", "type": "user"},
    ]

    for msg in welcome_messages:
        sse_data = format_sse_event(
            data={
                "type": msg["type"],
                "user": msg["user"],
                "content": msg["content"],
                "timestamp": datetime.now().isoformat()
            },
            event="chat_message"
        )
        yield sse_data

    # 模拟持续接收消息 (实际应用中这里会从消息队列获取)
    message_count = 0
    while True:
        # 检查客户端是否断开连接
        if await request.is_disconnected():
            break

        # 模拟随机用户发送消息
        if message_count < 20:  # 限制消息数量
            users = ["Charlie", "Diana", "Eve", "Frank"]
            user = random.choice(users)
            contents = [
                "今天天气真好!",
                "有人想讨论技术问题吗?",
                "SSE 真是太强大了!",
                "流式传输很有用",
                "我喜欢这个聊天系统",
                "大家晚上好!"
            ]

            sse_data = format_sse_event(
                data={
                    "type": "user",
                    "user": user,
                    "content": random.choice(contents),
                    "timestamp": datetime.now().isoformat()
                },
                event="chat_message",
                id=f"chat-{message_count}"
            )

            yield sse_data
            message_count += 1
            await asyncio.sleep(random.uniform(2, 5))


async def generate_heartbeat() -> AsyncGenerator[str, None]:
    """
    生成心跳信号

    保持连接活跃，常用于检测连接是否存活
    """
    while True:
        sse_data = format_sse_event(
            data={
                "type": "heartbeat",
                "timestamp": datetime.now().isoformat(),
                "server_status": "alive"
            },
            event="heartbeat",
            retry=5000  # 客户端在断开后5秒重连
        )

        yield sse_data
        await asyncio.sleep(5)  # 每5秒发送一次心跳


# ==================== API 路由 ====================

@app.get("/")
async def root():
    """根路径 - 返回服务器信息"""
    return {
        "name": "SSE Streaming Server",
        "version": "1.0.0",
        "endpoints": {
            "time_stream": "/stream/time",
            "stock_stream": "/stream/stocks",
            "task_progress": "/stream/progress/{task_id}",
            "chat_stream": "/stream/chat",
            "heartbeat": "/stream/heartbeat"
        }
    }


@app.get("/stream/time")
async def time_stream():
    """
    时间流端点

    客户端连接此端点接收每秒更新的时间戳
    """
    import requests
    url = "https://httpbin.org/post"
    data = {
        "name": "test_user",
        "message": "这是一条测试消息",
        "timestamp": str(time.time())
    }
    with requests.post(url, data=data, stream=True) as response:
        print(f"状态码: {response.status_code}")
        print(f"响应头: {dict(response.headers)}")
        print("\n流式读取响应内容:")

        buffer = b""
        for chunk in response.iter_content(chunk_size=1024, decode_unicode=True):
            if chunk:
                buffer += chunk.encode() if isinstance(chunk, str) else chunk
                print(f"  收到 chunk ({len(chunk)} bytes): {chunk[:100]}...")

        print(f"\n完整响应:\n{buffer.decode()}")


    return StreamingResponse(
        generate_time_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # 禁用 Nginx 缓冲
        }
    )


@app.get("/stream/stocks")
async def stock_stream():
    """
    股票数据流端点

    模拟实时股票行情推送，每500ms更新一次
    """
    return StreamingResponse(
        generate_stock_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )


@app.get("/stream/progress/{task_id}")
async def task_progress(task_id: str):
    """
    任务进度流端点

    Args:
        task_id: 任务ID

    返回指定任务ID的进度更新
    """
    return StreamingResponse(
        generate_task_progress(task_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )


@app.get("/stream/chat")
async def chat_stream(request: Request):
    """
    聊天消息流端点

    支持实时聊天消息推送
    """
    return StreamingResponse(
        generate_chat_stream(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )


@app.get("/stream/heartbeat")
async def heartbeat_stream():
    """
    心跳流端点

    定期发送心跳信号，用于保持连接活跃
    """
    return StreamingResponse(
        generate_heartbeat(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )


from urllib3.util.request import ChunksAndContentLength
import urllib3.connection
# 保存原始函数
origin = urllib3.connection.body_to_chunks

def patched_body_to_chunks(body, method, blocksize):
    ret = origin(body, method, blocksize)  # 使用保存的原始函数

    def wrapped_chunks(gen):
        for chunk in gen:
            HTTP_CTX.set(dict(data=chunk))
            yield chunk

    return ChunksAndContentLength(
        chunks=wrapped_chunks(ret.chunks),
        content_length=ret.content_length
    )

# ✅ 关键：直接修改 urllib3.util.request 模块的属性
urllib3.connection.body_to_chunks = patched_body_to_chunks


# ==================== 启动入口 ====================

if __name__ == "__main__":

    import uvicorn

    print("=" * 60)
    print("SSE Streaming Server 启动中...")
    print("=" * 60)
    print("\n可用端点:")
    print("  - GET /stream/time     - 时间流 (每秒更新)")
    print("  - GET /stream/stocks   - 股票数据流 (每500ms更新)")
    print("  - GET /stream/progress/{task_id} - 任务进度流")
    print("  - GET /stream/chat     - 聊天消息流")
    print("  - GET /stream/heartbeat - 心跳流 (每5秒)")
    print("\n运行客户端示例:")
    print("  python client.py")
    print("\n默认监听: http://127.0.0.1:8000")
    print("=" * 60)

    # 启动服务器
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8000,
        log_level="info"
    )
