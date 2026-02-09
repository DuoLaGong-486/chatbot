"""
内存优化解决方案压力测试客户端

这个脚本用于测试FastAPI应用的内存优化效果：
1. 测试高并发请求
2. 测试大请求体处理
3. 测试采样打印策略
4. 测试内存限制和熔断

使用方法：
    python test_client.py --host localhost --port 8000 --concurrency 50 --requests 100
"""
import asyncio
import argparse
import json
import random
import string
import time
import aiohttp
from typing import List, Dict
from dataclasses import dataclass
from datetime import datetime


@dataclass
class TestResult:
    """测试结果"""
    name: str
    total_requests: int
    successful: int
    failed: int
    avg_response_time_ms: float
    total_time_ms: float
    requests_per_second: float
    errors: List[str]


def generate_random_text(length: int = 100) -> str:
    """生成随机文本"""
    letters = string.ascii_letters + string.digits + " "
    return ''.join(random.choice(letters) for _ in range(length))


async def test_create_users(base_url: str, num_requests: int) -> TestResult:
    """测试创建用户接口"""
    print(f"  📝 测试创建用户接口 ({num_requests} 请求)...")
    
    start_time = time.time()
    successful = 0
    failed = 0
    response_times = []
    errors = []
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(num_requests):
            user_data = {
                "username": f"testuser_{random.randint(1000, 9999)}",
                "email": f"test{random.randint(1000, 9999)}@example.com",
                "password": "password123",
                "tags": ["test", "performance"]
            }
            
            async def make_request(idx, data):
                nonlocal successful, failed, response_times, errors
                req_start = time.time()
                try:
                    async with session.post(
                        f"{base_url}/users/",
                        json=data,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        elapsed = (time.time() - req_start) * 1000
                        response_times.append(elapsed)
                        
                        if response.status == 200:
                            successful += 1
                        elif response.status == 503:
                            errors.append("Service Unavailable (throttled)")
                            failed += 1
                        else:
                            failed += 1
                            error_text = await response.text()
                            errors.append(f"Status {response.status}: {error_text[:100]}")
                except Exception as e:
                    failed += 1
                    errors.append(f"Error: {str(e)[:100]}")
            
            tasks.append(make_request(i, user_data))
        
        await asyncio.gather(*tasks)
    
    total_time = (time.time() - start_time) * 1000
    
    return TestResult(
        name="Create Users",
        total_requests=num_requests,
        successful=successful,
        failed=failed,
        avg_response_time_ms=sum(response_times) / len(response_times) if response_times else 0,
        total_time_ms=total_time,
        requests_per_second=num_requests / (total_time / 1000) if total_time > 0 else 0,
        errors=errors[:5]  # 只保留前5个错误
    )


async def test_chat_endpoint(base_url: str, num_requests: int) -> TestResult:
    """测试聊天接口"""
    print(f"  💬 测试聊天接口 ({num_requests} 请求)...")
    
    start_time = time.time()
    successful = 0
    failed = 0
    response_times = []
    errors = []
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(num_requests):
            chat_data = {
                "messages": [
                    {"role": "user", "content": generate_random_text(50)},
                    {"role": "assistant", "content": "Previous response"}
                ],
                "max_tokens": 100,
                "temperature": 0.7
            }
            
            async def make_request(idx, data):
                nonlocal successful, failed, response_times, errors
                req_start = time.time()
                try:
                    async with session.post(
                        f"{base_url}/chat/",
                        json=data,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        elapsed = (time.time() - req_start) * 1000
                        response_times.append(elapsed)
                        
                        if response.status == 200:
                            successful += 1
                        elif response.status == 503:
                            errors.append("Service Unavailable (throttled)")
                            failed += 1
                        else:
                            failed += 1
                except Exception as e:
                    failed += 1
                    errors.append(f"Error: {str(e)[:100]}")
            
            tasks.append(make_request(i, chat_data))
        
        await asyncio.gather(*tasks)
    
    total_time = (time.time() - start_time) * 1000
    
    return TestResult(
        name="Chat Endpoint",
        total_requests=num_requests,
        successful=successful,
        failed=failed,
        avg_response_time_ms=sum(response_times) / len(response_times) if response_times else 0,
        total_time_ms=total_time,
        requests_per_second=num_requests / (total_time / 1000) if total_time > 0 else 0,
        errors=errors[:5]
    )


async def test_large_body(base_url: str, num_requests: int, body_size: int) -> TestResult:
    """测试大请求体处理"""
    print(f"  📦 测试大请求体 ({num_requests} 请求, {body_size} bytes)...")
    
    start_time = time.time()
    successful = 0
    failed = 0
    response_times = []
    errors = []
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        large_body = generate_random_text(body_size)
        
        for i in range(num_requests):
            async def make_request(idx, body):
                nonlocal successful, failed, response_times, errors
                req_start = time.time()
                try:
                    async with session.get(
                        f"{base_url}/test/large-body",
                        data=body.encode(),
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        elapsed = (time.time() - req_start) * 1000
                        response_times.append(elapsed)
                        
                        if response.status == 200:
                            successful += 1
                        else:
                            failed += 1
                except Exception as e:
                    failed += 1
                    errors.append(f"Error: {str(e)[:100]}")
            
            tasks.append(make_request(i, large_body))
        
        await asyncio.gather(*tasks)
    
    total_time = (time.time() - start_time) * 1000
    
    return TestResult(
        name=f"Large Body ({body_size} bytes)",
        total_requests=num_requests,
        successful=successful,
        failed=failed,
        avg_response_time_ms=sum(response_times) / len(response_times) if response_times else 0,
        total_time_ms=total_time,
        requests_per_second=num_requests / (total_time / 1000) if total_time > 0 else 0,
        errors=errors[:5]
    )


async def test_memory_monitor(base_url: str, num_requests: int) -> TestResult:
    """测试内存监控端点"""
    print(f"  📊 测试内存监控 ({num_requests} 请求)...")
    
    start_time = time.time()
    successful = 0
    failed = 0
    response_times = []
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(num_requests):
            async def make_request(idx):
                nonlocal successful, failed, response_times
                req_start = time.time()
                try:
                    async with session.get(
                        f"{base_url}/test/memory",
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as response:
                        elapsed = (time.time() - req_start) * 1000
                        response_times.append(elapsed)
                        
                        if response.status == 200:
                            successful += 1
                        else:
                            failed += 1
                except Exception as e:
                    failed += 1
            
            tasks.append(make_request(i))
        
        await asyncio.gather(*tasks)
    
    total_time = (time.time() - start_time) * 1000
    
    return TestResult(
        name="Memory Monitor",
        total_requests=num_requests,
        successful=successful,
        failed=failed,
        avg_response_time_ms=sum(response_times) / len(response_times) if response_times else 0,
        total_time_ms=total_time,
        requests_per_second=num_requests / (total_time / 1000) if total_time > 0 else 0,
        errors=[]
    )


async def get_optimization_stats(base_url: str) -> Dict:
    """获取优化统计信息"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{base_url}/_optimization_stats") as response:
            return await response.json()


async def get_health_status(base_url: str) -> Dict:
    """获取健康状态"""
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{base_url}/_health") as response:
            return await response.json()


async def run_stress_test(base_url: str, concurrency: int, requests_per_test: int):
    """运行压力测试"""
    print(f"\n🚀 开始压力测试")
    print(f"   Base URL: {base_url}")
    print(f"   并发数: {concurrency}")
    print(f"   每个测试请求数: {requests_per_test}")
    print("=" * 60)
    
    all_results = []
    
    # 测试前获取状态
    print("\n📍 测试前状态:")
    try:
        health = await get_health_status(base_url)
        print(f"   健康状态: {health.get('status', 'unknown')}")
        
        stats = await get_optimization_stats(base_url)
        mem = stats.get("memory_manager", {})
        print(f"   当前内存使用: {mem.get('current_usage', 0) / 1024 / 1024:.2f} MB")
        print(f"   活跃请求: {mem.get('active_requests', 0)}")
    except Exception as e:
        print(f"   ⚠️ 无法获取状态: {e}")
    
    print("\n" + "=" * 60)
    
    # 1. 测试创建用户
    result1 = await test_create_users(base_url, requests_per_test)
    all_results.append(result1)
    
    # 2. 测试聊天接口
    result2 = await test_chat_endpoint(base_url, requests_per_test)
    all_results.append(result2)
    
    # 3. 测试大请求体
    result3 = await test_large_body(base_url, requests_per_test, 5000)  # 5KB
    all_results.append(result3)
    
    # 4. 测试内存监控
    result4 = await test_memory_monitor(base_url, requests_per_test)
    all_results.append(result4)
    
    print("\n" + "=" * 60)
    print("\n📊 测试结果汇总:")
    print("=" * 60)
    
    total_successful = 0
    total_failed = 0
    total_time = 0
    
    for result in all_results:
        total_successful += result.successful
        total_failed += result.failed
        total_time = max(total_time, result.total_time_ms)
        
        print(f"\n🔹 {result.name}")
        print(f"   成功: {result.successful} | 失败: {result.failed}")
        print(f"   平均响应时间: {result.avg_response_time_ms:.2f} ms")
        print(f"   吞吐量: {result.requests_per_second:.2f} req/s")
        
        if result.errors:
            print(f"   错误示例:")
            for error in result.errors[:3]:
                print(f"      - {error}")
    
    print("\n" + "=" * 60)
    print(f"\n🎯 总体结果:")
    print(f"   总成功: {total_successful}")
    print(f"   总失败: {total_failed}")
    print(f"   成功率: {total_successful / (total_successful + total_failed) * 100:.2f}%")
    
    # 测试后获取状态
    print("\n📍 测试后状态:")
    try:
        health = await get_health_status(base_url)
        print(f"   健康状态: {health.get('status', 'unknown')}")
        
        stats = await get_optimization_stats(base_url)
        log_queue = stats.get("log_queue", {})
        print(f"   日志队列大小: {log_queue.get('queue_size', 0)}")
        print(f"   丢弃日志数: {log_queue.get('dropped', 0)}")
        
        mem = stats.get("memory_manager", {})
        print(f"   活跃请求: {mem.get('active_requests', 0)}")
    except Exception as e:
        print(f"   ⚠️ 无法获取状态: {e}")
    
    return all_results


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="内存优化压力测试客户端")
    parser.add_argument("--host", default="localhost", help="服务器地址")
    parser.add_argument("--port", type=int, default=8000, help="服务器端口")
    parser.add_argument("--concurrency", type=int, default=10, help="并发数")
    parser.add_argument("--requests", type=int, default=50, help="每个测试的请求数")
    
    args = parser.parse_args()
    
    base_url = f"http://{args.host}:{args.port}"
    
    await run_stress_test(base_url, args.concurrency, args.requests)


if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════╗
    ║     FastAPI 内存优化解决方案 - 压力测试客户端          ║
    ╠══════════════════════════════════════════════════════╣
    ║  测试内容:                                            ║
    ║  1. 创建用户接口性能                                  ║
    ║  2. 聊天接口性能                                     ║
    ║  3. 大请求体处理能力                                 ║
    ║  4. 内存监控端点性能                                 ║
    ╚══════════════════════════════════════════════════════╝
    """)
    
    asyncio.run(main())
