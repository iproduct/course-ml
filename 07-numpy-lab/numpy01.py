import time
from random import Random
import numpy as np



if __name__ == '__main__':
    arr = np.array([1, 2, 3, 4, 5])
    print(arr)
    print(np.__version__)

    arr = np.fromiter({1, 2, 3, 4, 5, 6}, dtype=np.uint64)
    arr = np.fromiter({1, 2, 3, 4, 5, 6}, dtype=np.float64)
    arr = np.fromiter({1, 2, 3, 4, 5, 6}, dtype='U1')
    arr = np.fromiter({1, 2, 3, 4, 5, 6}, dtype=(str, 1))
    print(arr)
    print(type(arr))

    arr = np.array([[1, 2, 3], [4, 5, 6]])
    print(arr)

    arr = np.array([[[1.5, 2, 3], [4, 5, 6]], [[1, 2, 3], [4, 5, 6]]])
    print(arr)
    print('number of dimensions :', arr.ndim)
    print('strides of dimensions :', arr.strides)
    print('shape :',  arr.shape)
    print('element type :',  arr.dtype)
    print('itemsize :',  arr.itemsize)

    for v in arr.flat:
        print(v, end=" | ", sep=", ")

    print()
    arr = np.array([1, 2, 3, 4], ndmin=5)
    print(arr)
    print('number of dimensions :', arr.ndim)

    # from function
    arr = np.fromfunction(lambda i, j: i * j, (5, 5), dtype=int)
    print(arr)
    rand = Random()
    arr = np.fromfunction(lambda i,j: i * j, (5, 5), dtype=int)
    print("\n", arr)

    arr = np.random.random((5,5))
    print("\n", arr)

    arr = np.random.randint(1, 100, (5,5), int)
    print("\n", arr)

