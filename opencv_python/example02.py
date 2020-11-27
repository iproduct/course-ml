import cv2
import tkinter as tk
import numpy as np

width = 512
height = 512

if __name__ == '__main__':
    root = tk.Tk()
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()

    img = cv2.imread("resources/lena.png", cv2.IMREAD_GRAYSCALE)
    # img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    print(img.shape)
    blurred = cv2.GaussianBlur(img, (11,11), 0)
    canny = cv2.Canny(img,90, 90)
    harris = cv2.cornerHarris(img, 2, 11 , 0.15)
    kernel = np.ones((5,5), np.uint8)
    dilated = cv2.dilate(canny, kernel, iterations=1)
    eroded = cv2.erode(dilated, kernel, iterations=1)
    cv2.imshow("Lena", img)
    cv2.imshow("Lena [edge detection]", canny)
    cv2.imshow("Lena [corner detection]", harris)
    cv2.imshow("Lena [image dilation]", dilated)
    cv2.imshow("Lena [image erosion]", eroded)

    # cv2.resizeWindow("Lena", width, height)
    # cv2.moveWindow("Lena", (screen_width - width) // 2, (screen_height - height) // 2 )
    cv2.waitKey(60000)
    cv2.destroyAllWindows()