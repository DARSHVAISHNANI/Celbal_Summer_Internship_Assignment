t = int(input())

for _ in range(t):
    try:
        a, b = input().split()
        result = int(a) // int(b)
        print(result)
    except (ZeroDivisionError, ValueError) as e:
        print("Error Code:", e)