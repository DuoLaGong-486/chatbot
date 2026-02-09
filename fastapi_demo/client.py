"""
同步 httpx 客户端 - 流式发送请求，流式接收响应
"""

import httpx
import json
import time


def stream_request_and_receive_response():
    """
    演示：流式发送请求 + 流式接收响应
    """
    print("=" * 60)
    print("演示：双向流式通信")
    print("=" * 60)

    with httpx.Client() as client:

        # 1. 构建要发送的流式数据（生成器）
        def request_data_generator():
            """生成要发送的数据块"""
            for i in range(5):
                data = {
                    "sequence": i,
                    "payload": f"这是第 {i} 个请求数据块",
                    "timestamp": time.time()
                }
                chunk = json.dumps(data, ensure_ascii=False) + "\n"
                print(f"发送请求块 #{i}: {len(chunk)} bytes")
                yield chunk.encode('utf-8')
                time.sleep(0.2)

        # 2. 发送流式请求，流式接收响应
        print("\n发送流式请求...")

        # 修正：使用 client.stream() 替代 client.post(..., stream=True)
        with client.stream(
                "POST",
                "http://localhost:8000/stream/bidirectional",
                content=request_data_generator()
        ) as response:

            print(f"响应状态码: {response.status_code}\n")

            # 3. 流式读取响应
            print("开始接收流式响应:")
            print("-" * 40)

            byte_buffer = b""

            for chunk in response.iter_bytes():
                byte_buffer += chunk

                # 按行处理数据（SSE 格式通常是 \n\n 分隔）
                while b"\n\n" in byte_buffer:
                    line, byte_buffer = byte_buffer.split(b"\n\n", 1)
                    line_text = line.decode('utf-8')

                    # 解析 SSE 格式
                    if line_text.startswith("data: "):
                        data_str = line_text[6:]  # 去掉 "data: " 前缀
                        try:
                            data = json.loads(data_str)
                            print(f"收到响应: {data}")
                        except json.JSONDecodeError:
                            print(f"收到原始数据: {line_text}")

            print("-" * 40)
            print("响应接收完成\n")


def test_stream_response_only():
    """
    仅测试流式接收响应
    """
    print("=" * 60)
    print("测试：仅接收流式响应")
    print("=" * 60)

    with httpx.Client() as client:
        response = client.get(
            "http://localhost:8000/stream/response",
            stream=True
        )

        print(f"状态码: {response.status_code}\n")
        print("接收流式数据:")

        for chunk in response.iter_bytes():
            text = chunk.decode('utf-8')
            print(f"  {text.strip()}")

        print("\n接收完成\n")


def test_stream_upload_only():
    """
    仅测试流式发送请求
    """
    print("=" * 60)
    print("测试：仅发送流式请求")
    print("=" * 60)

    with httpx.Client() as client:
        # 流式数据生成器
        def generate_data():
            for i in range(3):
                chunk = f"这是第 {i} 个数据块，内容量较大 " * 1
                print(f"  发送块 #{i}: {len(chunk)} chars")
                yield chunk.encode('utf-8')
                time.sleep(0.3)

        response = client.post(
            "http://localhost:8000/stream/upload",
            content=generate_data()
        )

        print(f"\n状态码: {response.status_code}")
        print(f"响应内容: {response.json()}\n")


def test_binary_stream():
    """
    测试二进制流式传输
    """
    print("=" * 60)
    print("测试：二进制流式传输")
    print("=" * 60)

    with httpx.Client() as client:
        response = client.get(
            "http://localhost:8000/stream/binary",
            stream=True
        )

        print(f"状态码: {response.status_code}\n")
        print("接收二进制数据:")

        total_bytes = 0
        for chunk in response.iter_bytes():
            total_bytes += len(chunk)
            print(f"  收到 {len(chunk)} bytes, 总计: {total_bytes}")

        print(f"\n总共接收: {total_bytes} bytes\n")


def upload_file_streaming():
    """
    模拟大文件流式上传
    """
    print("=" * 60)
    print("测试：大文件流式上传")
    print("=" * 60)

    with httpx.Client(timeout=30.0) as client:
        # 模拟大文件（使用生成器）
        def file_chunk_generator():
            """模拟文件分块"""
            for i in range(10):
                # 生成 1KB 的随机数据
                chunk = (f"[文件块 {i}] " + "A" * 1000 + "\n").encode()
                print(f"  上传块 #{i}: {len(chunk)} bytes")
                yield chunk
                time.sleep(0.2)

        response = client.post(
            "http://localhost:8000/stream/upload",
            content=file_chunk_generator()
        )

        print(f"\n状态码: {response.status_code}")
        print(f"响应: {response.json()}\n")


def run_all_tests():
    """
    运行所有测试
    """
    print("\n" + "=" * 60)
    print("开始测试...")
    print("=" * 60 + "\n")

    # 等待服务器启动
    import time
    print("等待服务器启动...")
    time.sleep(2)

    try:
        # 测试1: 流式接收响应
        # test_stream_response_only()

        # 测试2: 流式发送请求
        # test_stream_upload_only()

        # # 测试3: 双向流式通信
        stream_request_and_receive_response()
        #
        # # 测试4: 二进制流式
        # test_binary_stream()
        #
        # # 测试5: 大文件上传
        # upload_file_streaming()

    except httpx.ConnectError as e:
        print(f"连接错误: {e}")
        print("请确保服务器正在运行: python server.py")
    except Exception as e:
        print(f"错误: {e}")

import io
from typing import Iterator

# 假设这是你导入的原始类
# from httpx import _content (举例)

def patch_iterator_stream():
    """
    为指定的 stream_instance 注入拦截逻辑
    """
    # 保存原始的方法引用
    original_iter = httpx._content.IteratorByteStream.__iter__

    def hooked_iter(self) -> Iterator[bytes]:
        # 调用原始的生成器逻辑
        for chunk in original_iter(self):
            # 在 yield 给网络层之前，先写入我们的 buffer
            if not hasattr(self, "__xRasp__req"):
                self.__xRasp__req = io.BytesIO()
            self.__xRasp__req.write(chunk)
            yield chunk
        print(f"xRASP请求体: {self.__xRasp__req.getvalue().decode()}")

    # 替换实例的方法
    httpx._content.IteratorByteStream.__iter__ = hooked_iter


if __name__ == "__main__":
    patch_iterator_stream()

    run_all_tests()
