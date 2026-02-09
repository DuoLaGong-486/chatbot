
# ✅ 正确方式：直接修改模块属性
import urllib3
from urllib3.util.request import ChunksAndContentLength
import urllib3.connection

# 保存原始函数
origin = urllib3.connection.body_to_chunks

def patched_body_to_chunks(body, method, blocksize):
    ret = origin(body, method, blocksize)  # 使用保存的原始函数

    def wrapped_chunks(gen):
        for chunk in gen:
            print(f"chunk: {chunk}")
            yield chunk

    return ChunksAndContentLength(
        chunks=wrapped_chunks(ret.chunks),
        content_length=ret.content_length
    )

# ✅ 关键：直接修改 urllib3.util.request 模块的属性
urllib3.connection.body_to_chunks = patched_body_to_chunks

import requests
import json

url = "https://httpbin.org/post"

# 非流式：整个 body 一次性构建好
data = {"name": "Alice", "age": 30}

def gen():
    yield b"hello"
    yield b"world"

response = requests.post(url, json=data)  # 自动设置 Content-Type: application/json

print("Status:", response.status_code)
print("Response:", response.json()["json"])


