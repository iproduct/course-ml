import cv2 as cv

if __name__ == '__main__':
    print(f'OpenCV imported: {cv.version.opencv_version}')
    img = cv.imread("resources/lena.png")
    cv.imshow("Lena", img)

    cv.waitKey(0)
