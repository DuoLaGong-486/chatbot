"""
同步流式服务器 - 支持流式请求和流式响应
"""
import asyncio
import json
import time
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from typing import Generator

app = FastAPI(
    title="同步流式服务器",
    version="1.0.0"
)


# ============ 流式响应生成器 ============

def data_generator() -> Generator[str, None, None]:
    """生成流式数据"""
    for i in range(10):
        data = {
            "chunk": i,
            "message": f"第 {i} 个响应数据块",
            "timestamp": time.time()
        }
        yield f"data: {json.dumps(data)}\n\n"
        time.sleep(0.3)


def large_data_generator() -> Generator[bytes, None, None]:
    """生成大块二进制数据"""
    for i in range(50):
        chunk = f"[块 {i}] " + "x" * 200 + "\n"
        yield chunk.encode('utf-8')
        time.sleep(0.1)


# ============ API 端点 ============

@app.get("/stream/response")
def stream_response():
    """
    返回流式响应（Server-Sent Events 格式）
    """
    return StreamingResponse(
        data_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )


@app.get("/stream/binary")
def stream_binary():
    """
    返回二进制流式数据
    """
    return StreamingResponse(
        large_data_generator(),
        media_type="application/octet-stream"
    )


@app.post("/stream/upload")
async def stream_upload(request: Request): # 1. 改为 async def
    received_data = []
    total_size = 0
    chunk_count = 0

    print("\n--- 开始接收流式数据 ---")

    # body = await request.body()
    # print(f"请求体: {body.decode()}")

    # 2. 使用 async for
    async for chunk in request.stream():
        if chunk:
            chunk_count += 1
            chunk_len = len(chunk)
            total_size += chunk_len
            received_data.append(chunk)
            print(f"收到块 #{chunk_count}: {chunk_len} bytes")

    full_data = b"".join(received_data)
    return {
        "status": "success",
        "received_size": total_size,
        "received_data_size": len(full_data),
        "received_data": full_data.decode('utf-8', errors='ignore'),
        "chunk_count": chunk_count
    }


@app.post("/stream/bidirectional")
async def stream_bidirectional(request: Request):  # 改为 async def
    """
    接收流式请求，返回流式响应（双向流式）
    """
    received_chunks = []

    print("\n--- 开始接收请求数据 ---")

    # 1. 异步接收请求数据（关键修正：async for）
    async for chunk in request.stream():  # 改为 async for
        if chunk:
            received_chunks.append(chunk)
            print(f"收到请求块: {len(chunk)} bytes")

    print("--- 请求接收完成 ---\n")

    # 2. 生成流式响应（改为异步生成器，避免阻塞）
    async def response_generator():
        """根据请求内容生成流式响应"""
        full_data = b"".join(received_chunks)
        request_text = full_data.decode('utf-8', errors='ignore')

        # 处理请求数据
        words = request_text.split() if request_text else ["无数据"]

        # 流式返回处理结果
        for i, word in enumerate(words[:10]):
            response_data = {
                "processed_word": word,
                "word_index": i,
                "request_size": len(request_text)
            }
            yield f"data: {json.dumps(response_data)}\n\n"
            await asyncio.sleep(0.2)  # 改为异步 sleep，不阻塞事件循环

        # 结束标记
        yield f"data: {json.dumps({'status': 'complete', 'total_words': len(words[:10])})}\n\n"
    raise Exception("测试异常")
    return StreamingResponse(
        response_generator(),
        media_type="text/event-stream"
    )


@app.get("/health")
def health_check():
    """健康检查"""
    return {
        "status": "ok",
        "uptime": time.time()
    }


class ASGIRequest:
    """
    A barebones request class for ASGI applications. This class is mostly useful for
    managing the `receive` coroutine, which is how ASGI apps get the request body.
    """

    HTTP_REQUEST = "http.request"
    HTTP_DISCONNECT = "http.disconnect"

    def __init__(self, scope, original_receive):
        self.scope = scope
        self.original_receive = original_receive

        self._called = False
        self._body = None
        self._wsgi_environ = None

    async def contrast__receive(self):
        """
        This coroutine should be sent to the underlying ASGI application instead of the
        original `receive`. This allows us to capture the request body while still
        providing the ASGI app with a valid `receive`-like coroutine.
        """
        if self._called:
            # We call the real receive to exhaustion the first time `contrast__receive`
            # is called. On subsequent calls, we must defer to the behavior of the real
            # receive. Some frameworks (starlette) call `receive` in a loop for
            # streaming responses and check for the http.disconnect message.
            return await self.original_receive()

        body = await self.body()
        self._called = True
        return {
            "type": self.HTTP_REQUEST,
            "body": body,
            "more_body": False,
        }

    async def body(self):
        """
        Call the original `receive` coroutine and store the result. This coroutine can
        be called multiple times without adverse consequences.
        """
        if self._body is not None:
            return self._body

        body_parts = []
        more_body = True
        while more_body:
            event = await self.original_receive()
            if event["type"] == self.HTTP_REQUEST:
                body_parts.append(event.get("body", b""))
                more_body = event.get("more_body", False)
            else:
                more_body = False
        self._body = b"".join(body_parts)
        return self._body

import io
from typing import AsyncGenerator
from starlette.requests import Request
from starlette.types import Message

def patch_fastapi_request_stream():
    """
    为 starlette.requests.Request.stream 注入拦截逻辑
    实现被动记录 FastAPI 接收到的请求体
    """
    # 1. 保存原始的异步生成器方法引用
    original_stream = Request.stream

    async def hooked_stream(self: Request) -> AsyncGenerator[bytes, None]:
        # 2. 准备缓冲区 (挂载在 request 实例上)
        if not hasattr(self, "__xRasp__req"):
            self.__xRasp__req = io.BytesIO()

        # 3. 驱动原始的异步生成器
        async for chunk in original_stream(self):
            if chunk:
                # 在 yield 给业务逻辑之前，先记录
                self.__xRasp__req.write(chunk)
            yield chunk

        # 4. 当流结束时，打印或处理记录的数据
        # 注意：只有在流完全读完时才会走到这里
        try:
            full_body = self.__xRasp__req.getvalue().decode("utf-8")
            print(f"xRASP 拦截到异步请求体: {full_body}")
        except Exception as e:
            print(f"xRASP 解析失败: {e}")

    # 5. 执行替换
    Request.stream = hooked_stream


async def hooked_iterator(original_iterator):
    async for chunk in original_iterator:
            # --- 这里是你的 Hook 逻辑 ---
            print(f"Hook 拦截到数据块: {len(chunk)} bytes")
            # -------------------------
            yield chunk  # 必须原样 yield 出去，否则业务层收不到数据

# # 在初始化之后进行 Hook
# response = StreamingResponse(content=my_gen())
# response.body_iterator = hooked_iterator(response.body_iterator)


if __name__ == "__main__":
    # patch_fastapi_request_stream()
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)