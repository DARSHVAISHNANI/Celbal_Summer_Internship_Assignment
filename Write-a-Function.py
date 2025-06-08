def leap_year(number):
    if(number%4==0 and (number%400==0 or number%100!=0)):
        print(f"Year {number} is Leap Year.")
    else:
        print(f"Year {number} is not Leap Year.")

print("Enter the Year: ")
Year = int(input())

leap_year(Year)