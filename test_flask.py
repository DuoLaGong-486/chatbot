from flask import Flask, request
import sys
import os

# 创建应用实例
app = Flask(__name__)

# 配置应用
app.config.update({
    'TESTING': True,  # 简化测试环境
    'DEBUG': True  # 启用调试模式
})


@app.route('/debug', methods=['GET'])
def debug_endpoint():
    """调试端点 - 获取WSGI输入流"""
    # 这是第一个断点位置
    print("=" * 50)
    print("进入debug_endpoint函数")
    print(f"当前时间: {os.popen('time /T').read().strip()}")
    print(f"Python进程ID: {os.getpid()}")

    # 显示request基本信息
    print(f"请求方法: {request.method}")
    print(f"请求路径: {request.path}")

    # 检查request.environ
    environ = request.environ
    print(f"wsgi.input 存在: {'wsgi.input' in environ}")

    # 这是第二个断点位置 - 获取stream_obj
    stream_obj = environ['wsgi.input']
    print(f"stream_obj 类型: {type(stream_obj)}")

    # 这是第三个断点位置 - 返回响应前
    print("准备返回响应...")
    print("=" * 50)

    return {
        "class_name": str(type(stream_obj)),
        "mro": [c.__name__ for c in type(stream_obj).mro()],
        "endpoint": "debug_endpoint",
        "python_version": sys.version.split()[0]
    }


if __name__ == '__main__':
    """应用入口 - 确保调试器能够正确工作"""
    print("=" * 70)
    print("启动Flask调试应用")
    print(f"Python解释器: {sys.executable}")
    print(f"Python版本: {sys.version}")
    print(f"工作目录: {os.getcwd()}")
    print(f"应用文件: {__file__}")
    print(f"调试模式: {app.debug}")
    print("=" * 70)

    # 关键配置：禁用reloader和多线程
    app.run(
        debug=True,
        use_reloader=False,  # 禁用自动重载（导致调试问题的主要原因）
        threaded=False,  # 单线程运行
        processes=1,  # 单进程运行
        host='127.0.0.1',  # 仅本地访问
        port=5000  # 默认端口
    )