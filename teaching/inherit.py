class Calculus:
    def __int__(self, name):
        self.name = name

    def factorial(self, num):
        print(f"The factorial of {num}")

    def multiply(self, x, y):
        return x * y

#c = Calculus(name)


class Maths(Calculus):
    def __int__(self, name):
        super().__init__(name)
        self.lastName = "Bolaji"

    def addition(self):
        pass


class Mobile(Maths):
    def __init__(self, name):
        super().__init__(name)
        self.myFriend = name

    def find_factorial(self):
        self.factorial(23)

# M = Mobile("Labi")
#
# M.