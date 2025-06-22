from itertools import groupby

string_number=input()

for number,occurance in groupby(string_number):
    print((len(list(occurance)),int(number)),end=" ")