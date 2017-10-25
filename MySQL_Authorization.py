import os

def MySQL_Auth():
    startingDir = os.getcwd()
    os.chdir('C:/Users/tlack/Documents')
    token = None
    with open("password.txt") as I:
        token = I.read().strip()
    os.chdir(startingDir)
    return token