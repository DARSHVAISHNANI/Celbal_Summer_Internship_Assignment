from collections import Counter

def calculate_earnings():
    # Input reading
    n = int(input())  
    shoe_sizes = list(map(int, input().split()))  
    m = int(input())  
    
    shoe_counter = Counter(shoe_sizes)
    
    total_earnings = 0
    
    for _ in range(m):
        size, price = map(int, input().split())
        
        if shoe_counter[size] > 0:
            total_earnings += price
            shoe_counter[size] -= 1  
    
    
    print(total_earnings)

calculate_earnings()