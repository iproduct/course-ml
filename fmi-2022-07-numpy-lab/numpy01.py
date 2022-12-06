import numpy as np

if __name__ == '__main__':
    arr2d = np.array([[1, 2, 3, 4, 5],
                    [7, 8, 9, 10, 11]])
    arr3d = np.array([[[1, 2, 3], [4, 5, 6]], [[11, 12, 13], [14, 15, 16]]], dtype='S')
    print(arr3d)
    print(arr3d.ndim)
    print(arr3d.shape)
    print(arr3d[0, 1, 2])
    print(arr3d.dtype)

    # arr5d = np.array([1, 2, 3, 4], ndmin=5)
    # print(arr5d)
    # print(arr5d.shape)

    arr1 = np.array([1, 2, 3, 4, 5, 6, 7])
    slice1 = arr1[1:5]
    # slice1 = arr1.copy()[1:5]
    print(slice1)
    slice1[0] = 42
    print(arr1)

    reverse = arr1[::-1]
    print(reverse)

    slice3d = arr3d[:,0,2:0:-1]
    print(slice3d)

    arr = np.array([1, 2, 3, 4, 5])
    np.random.shuffle(arr)
    print(arr)