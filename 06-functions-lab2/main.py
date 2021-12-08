
x = 42 # global

def f():
    x = 108
    def g():
        nonlocal x
        x = 15
        print(f'In f:, {x}')
    g()
    print(f'In g:, {x}')


if __name__ == '__main__':
    f()
    print("In main:", x)

