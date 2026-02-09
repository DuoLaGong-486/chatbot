"""
urllib3 HTTPResponse.stream 方法的补丁
在每次yield data时打印数据
"""

import functools
from urllib3.response import HTTPResponse


def patch_stream():
    """
    为 HTTPResponse.stream 方法添加打印功能的补丁
    """
    original_stream = HTTPResponse.stream

    @functools.wraps(original_stream)
    def patched_stream(self, amt=2**16, decode_content=None):
        for data in original_stream(self, amt, decode_content):
            print(f"[STREAM DATA] {len(data)} bytes: {data}")
            yield data

    HTTPResponse.stream = patched_stream


def unpatch_stream():
    """恢复原始的stream方法"""
    HTTPResponse.stream = original_stream


# 保存原始方法
original_stream = HTTPResponse.stream
