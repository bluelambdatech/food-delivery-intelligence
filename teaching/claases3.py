# __init__
# __name__
# __add__
# __str__
# print(__name__)
#
# if __name__ == "__main__":
#     print("omolewa")

# __add__

# print(2 + 3)
#
# x = 2
# y =3
# print(x.__add__(y))

class practise:
    def __init__(self, x):
        self.x=x

    def __add__(self, other):
        return f"The sum of {self.x} and {other.x}: {10 * self.x + other.x}"

    def __str__(self):
        return f"{self.x}, my name is Omolewa"

    def __len__(self):
        return 100


x1 = practise([1, 2, 3, 4, 5])
# x1 = practise(4)
# x2 = practise(5)
# x3 = practise(10)
#print(x1 + x3)

print(len(x1))
#
# n = 10
# print(n)

#print(x1.__dir__())