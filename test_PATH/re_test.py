import re
import time

_PATH_PATTERN = re.compile(r'([kai])(?::([^/]+))?')
test_path = "k:users/i:0/k:profile/k:settings/k:security/k:mfa_enabled"

# 正则方案
start = time.perf_counter()
for _ in range(100000):
    matches = _PATH_PATTERN.findall(test_path)
regex_time = time.perf_counter() - start

# 字符串分割方案
start = time.perf_counter()
for _ in range(100000):
    parts = test_path.split('/')
split_time = time.perf_counter() - start

print(f"正则: {regex_time:.3f}s")
print(f"分割: {split_time:.3f}s")
print(f"比例: {split_time/regex_time:.2f}x")