import cv2

if __name__ == '__main__':
    # read a color image
    img1 = cv2.imread("resources/lena.png")
    print(img1.shape)
    cv2.imshow("lena", img1)
    img2 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
    cv2.imshow("lena_gray", img2)
    key = cv2.waitKey(0)
    print(f"key '{chr(key)}' pressed")
    cv2.destroyAllWindows()