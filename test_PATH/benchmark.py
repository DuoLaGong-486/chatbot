import time
from dataclasses import dataclass, field
from typing import List, Dict, Any
from glom import glom as glom_extract, T
from fast_path import FastPath

# --- 数据结构定义 ---
@dataclass
class Node:
    name: str
    child: Any = None

def run_benchmarks(iterations=100000):
    # 场景 1: 纯字典深层嵌套 (8层)
    dict_data = {"a": {"b": {"c": {"d": {"e": {"f": {"g": {"h": "win"}}}}}}}}
    path_dict = "k:a/k:b/k:c/k:d/k:e/k:f/k:g/k:h"
    spec_dict = T["a"]["b"]["c"]["d"]["e"]["f"]["g"]["h"]

    # 场景 2: 纯对象深层嵌套 (8层)
    obj_data = Node("1", Node("2", Node("3", Node("4", Node("5", Node("6", Node("7", Node("8", "win"))))))))
    path_obj = "a:child/a:child/a:child/a:child/a:child/a:child/a:child/a:child"
    spec_obj = T.child.child.child.child.child.child.child.child

    # 场景 3: 极限混合嵌套 (k:a:k:a 穿插)
    # 路径: dict -> obj -> dict -> obj -> attr
    mixed_data = {"root": Node("mid", {"data": Node("leaf", "win")})}
    path_mixed = "k:root/a:child/k:data/a:child"
    spec_mixed = T["root"].child["data"].child

    test_cases = [
        ("纯字典 (8层)", dict_data, path_dict, spec_dict),
        ("纯对象 (8层)", obj_data, path_obj, spec_obj),
        ("极限混合 (k:a:k)", mixed_data, path_mixed, spec_mixed),
    ]

    print(f"{'测试场景':<15} | {'FastPath (μs)':>12} | {'glom (μs)':>10} | {'提速比':>8}")
    print("-" * 60)

    total_speedup = 0

    for name, data, p_str, g_spec in test_cases:
        fp = FastPath(p_str)
        
        # 预热
        for _ in range(1000):
            fp.extract(data)
            glom_extract(data, g_spec)

        # 测试 FastPath
        t0 = time.perf_counter()
        for _ in range(iterations):
            fp.extract(data)
        fp_time = (time.perf_counter() - t0) / iterations * 1e6

        # 测试 glom
        t1 = time.perf_counter()
        for _ in range(iterations):
            glom_extract(data, g_spec)
        glom_time = (time.perf_counter() - t1) / iterations * 1e6

        speedup = glom_time / fp_time
        total_speedup += speedup
        print(f"{name:<15} | {fp_time:>12.2f} | {glom_time:>10.2f} | {speedup:>8.1f}x")

    print("-" * 60)
    print(f"所有场景平均提速: {total_speedup / len(test_cases):.1f}x")

if __name__ == "__main__":
    run_benchmarks()