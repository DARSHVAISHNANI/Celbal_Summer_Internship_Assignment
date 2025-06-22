import re

t = int(input())

for _ in range(t):
    pattern = input()
    try:
        re.compile(pattern)
        print(True)
    except re.error:
        print(False)