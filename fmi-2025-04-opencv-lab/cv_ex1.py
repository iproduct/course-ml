import cv2

if __name__ == "__main__":
    img1 = cv2.imread('lena.bmp')
    print(img1.shape)
    print(img1.dtype)
    print(type(img1))
    cv2.imshow('Lena', img1)
    cv2.waitKey(0)
    cv2.destroyAllWindows()
