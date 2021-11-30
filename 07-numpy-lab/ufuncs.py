import numpy as np

if __name__ == '__main__':
    x = [1, 2, 3, 4]
    y = [4, 5, 6, 7]
    z = []

    for i, j in zip(x, y):
      z.append(str(i) + str(j))
    print(z)

    def myconcat(x, y):
        return int(str(x) + str(y))
    uconcat = np.frompyfunc(myconcat, 2, 1)
    arrx = np.array(x).reshape(2, 2)
    arry = np.array(y).reshape(2, 2)
    print(arrx)
    print(arry)
    print(uconcat(arrx, arry))