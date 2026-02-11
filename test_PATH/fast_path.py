"""
高性能路径提取器 - 精简版
=====================

优化点：
1. 编译阶段：使用预编译正则表达式，减少字符串操作
2. 运行时：将访问器内联，消除函数调用开销
3. 统一索引访问（i: 同时支持 list/tuple/str）

支持的类型：
- k:key      -> 字典键访问
- a:attr     -> 对象属性访问
- i:0        -> 索引访问（list/tuple/str）
"""

import re
from functools import lru_cache
from typing import Any, List, Tuple, Optional


# ============ 预编译正则表达式 ============
_PATH_PATTERN = re.compile(r'([kai])(?::([^/]+))?')

# 操作码常量
_OP_DICT_KEY = 0
_OP_ATTR = 1
_OP_INDEX = 2


class FastPath:
    """
    高性能路径提取器
    
    通过预编译路径字符串，避免运行时的类型猜测和字符串分割。
    """
    __slots__ = ('_steps', '_path_str')
    
    def __init__(self, path_str: str):
        self._path_str = path_str
        self._steps: List[Tuple[int, Any]] = self._compile(path_str)
    
    # def _compile(self, path_str: str) -> List[Tuple[int, Any]]:
    #     """
    #     编译路径字符串为操作码链。
    #
    #     语法:
    #       k:key_name  -> 字典 Key
    #       a:attr_name -> 对象 Attribute
    #       i:0         -> 索引访问 (list/tuple/str)
    #     """
    #     steps = []
    #     if not path_str:
    #         return steps
    #
    #     matches = _PATH_PATTERN.findall(path_str)
    #
    #     if not matches:
    #         raise ValueError(f"Invalid path '{path_str}'")
    #
    #     for prefix, value in matches:
    #         if prefix == 'k':
    #             if not value:
    #                 raise ValueError("Missing key name after 'k:'")
    #             steps.append((_OP_DICT_KEY, value))
    #         elif prefix == 'a':
    #             if not value:
    #                 raise ValueError("Missing attribute name after 'a:'")
    #             steps.append((_OP_ATTR, value))
    #         elif prefix == 'i':
    #             if not value:
    #                 raise ValueError("Missing index after 'i:'")
    #             steps.append((_OP_INDEX, int(value)))
    #
    #     return steps

    def _compile(self, path_str: str):
        steps = []
        for part in path_str.split('/'):
            if not part:
                continue
            prefix = part[0]
            value = part[2:]  # 跳过 "k:" / "a:" / "i:"
            if prefix == 'k':
                steps.append((_OP_DICT_KEY, value))
            elif prefix == 'a':
                steps.append((_OP_ATTR, value))
            elif prefix == 'i':
                steps.append((_OP_INDEX, int(value)))
        return steps
    
    def extract(self, data: Any, default: Any = None, safe: bool = False) -> Any:
        """运行时执行提取逻辑"""
        current = data
        
        try:
            for op, arg in self._steps:
                if op == _OP_DICT_KEY:
                    current = current[arg]
                elif op == _OP_ATTR:
                    current = getattr(current, arg)
                elif op == _OP_INDEX:
                    current = current[arg]
            
            return current
            
        except (KeyError, IndexError, AttributeError, TypeError):
            if safe:
                return default
            raise
    
    def extract_optional(self, data: Any) -> Optional[Any]:
        """安全模式提取"""
        return self.extract(data, default=None, safe=True)
    
    def __repr__(self):
        return f"<FastPath path='{self._path_str}'>"


@lru_cache(maxsize=2048)
def get_fast_path(path_str: str) -> FastPath:
    """带缓存的工厂函数"""
    return FastPath(path_str)


def extract(data: Any, path: str, default: Any = None) -> Any:
    """一行代码提取嵌套数据"""
    return get_fast_path(path).extract(data, default)


def safe_extract(data: Any, path: str) -> Optional[Any]:
    """安全提取，不抛异常"""
    return get_fast_path(path).extract_optional(data)


if __name__ == "__main__":
    # 测试
    test_data = {
        "user": [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
    }

    print(extract(test_data, "k:user/i:0/k:name"))  # Alice
    print(safe_extract(test_data, "k:missing"))     # None