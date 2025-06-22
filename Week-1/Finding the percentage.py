n = int(input())

marks_of_student = {}

for _ in range(n):
    name, *line = input().split()
    scores = list(map(float, line))
    marks_of_student[name] = scores
query_name = input()

for k,v in marks_of_student.items():
    if k == query_name:
        total = 0
        for i in v:
            total += i
        average = total/len(scores)

print("{:.2f}".format(average))