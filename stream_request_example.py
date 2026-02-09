"""
使用 requests 库请求并处理流式响应的示例
"""

import requests
import time


def stream_post_example():
    """
    向 httpbin.org/post 发送 POST 请求并流式读取响应
    """
    url = "https://httpbin.org/post"
    
    # 准备要发送的数据
    data = {
        "name": "test_user",
        "message": "这是一条测试消息",
        "timestamp": str(time.time())
    }
    
    print("=" * 50)
    print("发送 POST 请求到:", url)
    print("数据:", data)
    print("=" * 50)
    
    # 方式1: 使用 with 语句（推荐，自动关闭）
    print("\n【方式1: 使用 with 语句】")
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
    
    print("\n" + "=" * 50)


def stream_post_manual_close():
    """
    手动管理连接的流式请求
    """
    url = "https://httpbin.org/post"
    data = {"key": "value", "number": 123}
    
    print("\n【方式2: 手动管理连接】")
    
    session = requests.Session()
    
    try:
        response = session.post(url, data=data, stream=True)
        
        print(f"状态码: {response.status_code}")
        
        # 流式读取
        print("流式内容:")
        for i, chunk in enumerate(response.iter_content(chunk_size=256)):
            print(f"  [{i}] {chunk.decode('utf-8')[:80]}...")
            
            # 模拟提前退出
            if i >= 2:
                print("  ... 提前退出循环")
                break
        
        # 确保关闭响应（释放连接回池）
        response.close()
        print("\n响应已关闭，连接已释放回池")
        
    finally:
        # 关闭 session（清空连接池）
        session.close()
        print("Session 已关闭")


def stream_with_real_time_processing():
    """
    模拟实时处理流式数据的场景
    """
    print("\n【方式3: 实时处理流式数据】")
    
    url = "https://httpbin.org/post"
    
    def process_chunk(chunk_data):
        """模拟处理每个数据块"""
        # 模拟耗时操作
        time.sleep(0.1)
        return f"[处理完成] {len(chunk_data)} bytes"
    
    with requests.post(url, data={"action": "streaming_test"}, stream=True) as resp:
        print("开始流式处理...")
        
        total_size = 0
        for i, chunk in enumerate(resp.iter_content(chunk_size=512)):
            processed = process_chunk(chunk)
            total_size += len(chunk)
            print(f"  {processed}")
        
        print(f"\n处理完成，总共 {total_size} bytes")


if __name__ == "__main__":
    # 运行所有示例
    stream_post_example()
    stream_post_manual_close()
    stream_with_real_time_processing()
    
    print("\n" + "=" * 50)
    print("所有示例运行完成！")
