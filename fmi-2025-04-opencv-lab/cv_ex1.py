import cv2

if __name__ == "__main__":
    img1 = cv2.imread('lena.bmp')
    print(img1.shape)
    print(img1.dtype)
    print(type(img1))
    cv2.imshow('Lena', img1)
    img2 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
    cv2.imshow('Lena_Grayscale', img2)
    while cv2.waitKey(10) != ord('q'):
        pass
    cv2.destroyAllWindows()
