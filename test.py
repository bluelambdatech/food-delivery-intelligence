import argparse

parser = argparse.ArgumentParser(
                    prog='ProgramName',
                    description='What the program does',
                    epilog='Text at the bottom of help')


parser.add_argument('-f', '--filename')           # positional argument
parser.add_argument('-c', '--count')      # option that takes a value
parser.add_argument('-b', '--bucketname')

args = parser.parse_args()
print(args)




