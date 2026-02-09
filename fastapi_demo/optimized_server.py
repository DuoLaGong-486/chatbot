"""
FastAPI 内存优化完整集成示例

这个示例展示了如何将内存优化解决方案集成到实际的FastAPI项目中。

核心功能：
1. 全局内存限制 - 类似Java线程池的容量控制
2. 异步日志队列 - 不阻塞事件循环
3. 请求体大小限制 - 单请求+全局双重限制
4. 采样打印 - 减少日志量
5. 内存监控 - 自动限流熔断

使用方法：
1. 直接运行此文件
2. 访问 http://localhost:8000/docs 测试API
3. 访问 http://localhost:8000/_optimization_stats 查看优化效果
4. 访问 http://localhost:8000/_health 查看健康状态
"""

import asyncio
import random
import string
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import uvicorn

# 导入优化解决方案
from fastapi_memory_solution import (
    create_optimized_middleware,
    setup_fastapi_app,
    AsyncLogQueue,
    GlobalMemoryManager,
    SamplingStrategy,
    MemoryMonitor,
    BufferPool,
    LogUtils,
    MemoryConfig
)


# ============ 1. 创建FastAPI应用 ============

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时
    print("🚀 应用启动，初始化内存优化组件...")
    yield
    # 关闭时
    print("🛑 应用关闭，清理资源...")
    AsyncLogQueue.get_instance().flush()
    GlobalMemoryManager.get_instance().shutdown()
    MemoryMonitor.get_instance().shutdown()


app = FastAPI(
    title="FastAPI Memory Optimized Demo",
    description="展示内存优化解决方案的实际应用",
    version="1.0.0",
    lifespan=lifespan
)


# ============ 2. 集成内存优化中间件 ============

# 创建优化中间件
middleware = create_optimized_middleware(
    app,
    max_body_size_mb=2.0,          # 最大请求体2MB
    log_body_size_kb=50.0,         # 日志记录最多50KB
    sample_rate=0.2,               # 20%的请求会打印完整日志
    memory_limit_mb=256,           # 内存限制256MB
    max_concurrent_requests=100,   # 最大100并发
    log_workers=2                  # 2个日志工作线程
)


# ============ 3. 定义API模型 ============

class UserCreate(BaseModel):
    """用户创建请求模型"""
    username: str = Field(..., min_length=3, max_length=50)
    email: str = Field(..., max_length=100)
    password: str = Field(..., min_length=6)
    tags: List[str] = Field(default_factory=list)


class UserResponse(BaseModel):
    """用户响应模型"""
    id: int
    username: str
    email: str
    tags: List[str]
    created_at: str


class ChatMessage(BaseModel):
    """聊天消息模型"""
    role: str  # "user" or "assistant"
    content: str


class ChatRequest(BaseModel):
    """聊天请求模型"""
    messages: List[ChatMessage]
    max_tokens: int = Field(default=100, ge=1, le=1000)
    temperature: float = Field(default=0.7, ge=0.0, ge=1.0)


class ChatResponse(BaseModel):
    """聊天响应模型"""
    response: str
    tokens_used: int
    processing_time_ms: float


# ============ 4. 模拟数据 ============

# 模拟数据库
users_db = []
user_id_counter = 1


def generate_random_text(length: int = 100) -> str:
    """生成随机文本用于测试"""
    letters = string.ascii_letters + string.digits + " "
    return ''.join(random.choice(letters) for _ in range(length))


# ============ 5. API端点 ============

@app.post("/users/", response_model=UserResponse)
async def create_user(user: UserCreate):
    """
    创建用户
    
    这个端点会：
    1. 接收请求体（自动限制大小）
    2. 捕获请求体用于日志
    3. 异步打印日志（不阻塞）
    4. 自动限流和熔断
    """
    global user_id_counter
    
    # 模拟业务处理
    await asyncio.sleep(0.1)  # 模拟数据库操作
    
    # 创建用户
    new_user = {
        "id": user_id_counter,
        "username": user.username,
        "email": user.email,
        "tags": user.tags,
        "created_at": datetime.now().isoformat()
    }
    
    users_db.append(new_user)
    user_id_counter += 1
    
    # 使用便捷工具记录日志
    LogUtils.async_log(
        f"Created user: {user.username}",
        level="info",
        extra={"user_id": new_user["id"], "action": "create_user"}
    )
    
    return new_user


@app.get("/users/{user_id}", response_model=UserResponse)
async def get_user(user_id: int):
    """获取用户详情"""
    # 模拟业务处理
    await asyncio.sleep(0.05)
    
    # 查找用户
    for user in users_db:
        if user["id"] == user_id:
            return user
    
    # 使用便捷工具记录错误日志
    LogUtils.async_log(
        f"User not found: {user_id}",
        level="warning",
        extra={"user_id": user_id, "action": "get_user"}
    )
    
    return {"error": "User not found"}


@app.post("/chat/", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    模拟聊天接口
    
    这个端点展示了：
    1. 处理较大的请求体
    2. 流式响应的处理
    3. 采样打印策略
    """
    start_time = time.time()
    
    # 模拟LLM处理
    await asyncio.sleep(0.2)
    
    # 构建响应
    response_text = f"Echo: {request.messages[-1].content}"
    tokens_used = len(response_text) // 4  # 估算token数
    processing_time = (time.time() - start_time) * 1000
    
    # 记录日志（可能被采样）
    LogUtils.async_log(
        f"Chat request processed",
        level="info",
        extra={
            "messages_count": len(request.messages),
            "tokens_used": tokens_used,
            "processing_time_ms": processing_time
        }
    )
    
    return {
        "response": response_text,
        "tokens_used": tokens_used,
        "processing_time_ms": processing_time
    }


@app.post("/stream/chat")
async def stream_chat(request: ChatRequest):
    """
    流式聊天接口
    
    展示如何处理流式响应，同时保持内存优化
    """
    async def generate_response():
        """生成流式响应"""
        base_message = request.messages[-1].content if request.messages else "Hello"
        
        for i in range(5):
            chunk = f"Chunk {i}: {base_message} - {generate_random_text(20)}\n"
            yield chunk
            await asyncio.sleep(0.1)  # 模拟流式生成
    
    return StreamingResponse(
        generate_response(),
        media_type="text/plain"
    )


@app.get("/test/large-body")
async def test_large_body(request: Request):
    """
    测试大请求体处理
    
    展示当请求体超过限制时的处理
    """
    # 从scope获取body（已经被中间件处理）
    body = request.scope.get("body", b"")
    
    return {
        "body_size": len(body),
        "body_preview": body[:100].decode('utf-8', errors='ignore') + "...",
        "message": "Request processed successfully"
    }


@app.get("/test/memory")
async def test_memory():
    """
    内存压力测试端点
    
    模拟高并发场景，测试内存优化效果
    """
    # 模拟一些内存使用
    test_data = generate_random_text(10000)  # 10KB
    
    await asyncio.sleep(0.01)
    
    # 异步记录日志
    LogUtils.async_log(
        f"Memory test - allocated {len(test_data)} bytes",
        level="debug",
        extra={"test_type": "memory"}
    )
    
    return {
        "status": "ok",
        "data_size": len(test_data)
    }


# ============ 6. 健康检查和统计端点 ============

@app.get("/")
async def root():
    """根路径"""
    return {
        "message": "Welcome to FastAPI Memory Optimized Demo",
        "docs": "/docs",
        "stats": "/_optimization_stats",
        "health": "/_health"
    }


@app.get("/_optimization_stats")
async def get_stats():
    """
    获取优化统计信息
    
    展示各个优化组件的运行状态：
    - 日志队列状态
    - 内存管理状态
    - 采样策略状态
    - 内存监控状态
    """
    stats = middleware.get_all_stats()
    
    # 添加时间戳
    stats["timestamp"] = datetime.now().isoformat()
    
    return stats


@app.get("/_health")
async def health_check():
    """健康检查端点"""
    stats = middleware.get_all_stats()
    
    # 计算健康状态
    memory = stats["memory_monitor"]
    mem_manager = stats["memory_manager"]
    
    is_healthy = (
        not memory["is_throttled"] and
        mem_manager["usage_percent"] < 80 and
        mem_manager["concurrent_percent"] < 80
    )
    
    return {
        "status": "healthy" if is_healthy else "degraded",
        "checks": {
            "memory_monitor": {
                "status": "ok" if not memory["is_throttled"] else "throttled",
                "current_mb": memory["current_memory_mb"],
                "limit_mb": memory["memory_limit_mb"]
            },
            "memory_manager": {
                "status": "ok",
                "usage_percent": mem_manager["usage_percent"],
                "concurrent_percent": mem_manager["concurrent_percent"],
                "active_requests": mem_manager["active_requests"]
            },
            "log_queue": {
                "status": "ok" if stats["log_queue"]["queue_size"] < 1000 else "busy",
                "queue_size": stats["log_queue"]["queue_size"],
                "dropped": stats["log_queue"]["dropped"]
            }
        }
    }


# ============ 7. 压力测试端点 ============

@app.post("/stress-test/{num_requests}")
async def stress_test(num_requests: int):
    """
    压力测试端点
    
    测试高并发下的内存优化效果
    
    Args:
        num_requests: 并发请求数量（1-100）
    """
    num_requests = min(max(num_requests, 1), 100)  # 限制范围
    
    results = []
    
    async def make_request(i: int):
        """发送单个测试请求"""
        start = time.time()
        
        try:
            # 发送测试请求
            body = {"test_id": i, "data": generate_random_text(100)}
            
            # 这里模拟并发处理
            await asyncio.sleep(random.uniform(0.01, 0.1))
            
            elapsed = time.time() - start
            
            results.append({
                "request_id": i,
                "status": "success",
                "elapsed_ms": elapsed * 1000
            })
            
        except Exception as e:
            results.append({
                "request_id": i,
                "status": "error",
                "error": str(e)
            })
    
    # 并发执行
    tasks = [make_request(i) for i in range(num_requests)]
    await asyncio.gather(*tasks)
    
    return {
        "total_requests": num_requests,
        "successful": sum(1 for r in results if r["status"] == "success"),
        "failed": sum(1 for r in results if r["status"] == "error"),
        "avg_elapsed_ms": sum(r.get("elapsed_ms", 0) for r in results) / len(results),
        "results": results[:10]  # 只返回前10个结果
    }


# ============ 8. 启动应用 ============

if __name__ == "__main__":
    """
    启动FastAPI服务器
    
    配置：
    - host: 0.0.0.0 (允许外部访问)
    - port: 8000
    - reload: False (生产环境建议关闭)
    """
    print("""
    🚀 FastAPI Memory Optimized Demo
    =================================
    
    访问以下链接：
    - API文档: http://localhost:8000/docs
    - 健康检查: http://localhost:8000/_health
    - 优化统计: http://localhost:8000/_optimization_stats
    
    内存优化特性：
    ✅ 全局内存限制（10MB）
    ✅ 单请求限制（100KB）
    ✅ 异步日志队列（2线程）
    ✅ 采样打印（20%）
    ✅ 内存监控和限流
    
    """)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=False,
        workers=1
    )
