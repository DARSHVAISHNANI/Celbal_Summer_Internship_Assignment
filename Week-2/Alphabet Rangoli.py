import string

def rangoli(size):
    alphabet = string.ascii_lowercase
    lines = []

    for i in range(size):
        left = alphabet[size-1:i:-1]  
        center = alphabet[i]          
        right = alphabet[i+1:size]    
        row = '-'.join(left + center + right)
        lines.append(row.center(4*size-3, '-'))  

    full_rangoli = lines[::-1][1:] + lines
    return '\n'.join(full_rangoli)

# Input
n = int(input())
# Output
print(rangoli(n))