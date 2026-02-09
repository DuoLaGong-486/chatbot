from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Literal
import threading
from langchain_community.chat_models import ChatZhipuAI
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage, BaseMessage
import os

from utils import HTTP_CONTEXT

# 设置 API 密钥（请确保密钥有效）
os.environ["ZHIPUAI_API_KEY"] = "b586e5b05d504592a46cebc8890fa549.WjLyFDuIZ3gITKUq"

app = FastAPI(title="LangChain Sync Stream Chatbot")

# 初始化模型（使用 ChatZhipuAI 直接实例化，避免 init_chat_model 兼容性问题）
llm = ChatZhipuAI(model="glm-4", temperature=0.5)

class MessageSchema(BaseModel):
    role: Literal["system", "user", "assistant"]
    content: str

class ChatRequest(BaseModel):
    messages: List[MessageSchema]

def convert_to_langchain_messages(messages: List[MessageSchema]) -> List[BaseMessage]:
    role_map = {
        "system": SystemMessage,
        "user": HumanMessage,
        "assistant": AIMessage
    }
    return [role_map[m.role](content=m.content) for m in messages]

@app.post("/chat/stream")
def chat_stream(request: ChatRequest):
    """同步流式响应：使用 llm.stream 返回生成器"""
    lc_messages = convert_to_langchain_messages(request.messages)

    def event_generator():
        # HTTP_CONTEXT.set({"response_chunks": [], "request": None})
        for chunk in llm.stream(lc_messages):
            print(f"Stream in thread: {threading.get_ident()}")
            if chunk.content:
                # 可选：按 SSE 格式发送（如 data: ...\n\n）
                # 这里简化为纯文本流；若需标准 SSE，可改为：
                # yield f"data: {chunk.content}\n\n"
                yield chunk.content
            else:
                print(HTTP_CONTEXT.get())

    return StreamingResponse(event_generator(), media_type="text/plain; charset=utf-8")

@app.post("/chat/invoke")
async def chat_invoke(request: ChatRequest):
    """异步非流式调用"""
    lc_messages = convert_to_langchain_messages(request.messages)
    response = await llm.ainvoke(lc_messages)
    return {"role": "assistant", "content": response.content}

# --- 补全部分：main 入口 ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
