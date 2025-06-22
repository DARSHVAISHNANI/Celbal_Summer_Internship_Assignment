def merge_the_tools(s, k):
    for i in range(0, len(s), k):
        substring = s[i:i+k]
        
        seen = set()
        result = ''
        
        for char in substring:
            if char not in seen:
                seen.add(char)
                result += char
        print(result)