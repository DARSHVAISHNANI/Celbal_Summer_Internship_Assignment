from itertools import combinations

letters = input().split()
K = int(input())

all_Combinations = list(combinations(letters, K))

count_with_a = 0

for i in all_Combinations:
    if('a' in i):
        count_with_a+=1
        
total_combinations = len(all_Combinations)

probability = count_with_a/total_combinations

print(f"{probability:.3f}")