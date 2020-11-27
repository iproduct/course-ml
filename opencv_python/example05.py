import cv2
from cv2 import *
import tkinter as tk
import numpy as np

width = 512
height = 512

if __name__ == '__main__':
    root = tk.Tk()
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()

    # img = cv2.imread("resources/lena.png", cv2.IMREAD_GRAYSCALE)
    # img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    img = np.zeros((512, 512, 3), np.uint8)
    img[:] = 255, 0, 0
    print(img.shape)

    cv2.line(img, (0,0), (400, 400), (0, 255, 0), 5, LINE_AA )
    putText(img, "Open CV", (200, 100), QT_FONT_NORMAL, 1, (0, 0, 255), 3, LINE_AA)

    cv2.imshow("Canvas", img)


    # cv2.resizeWindow("Lena", width, height)
    # cv2.moveWindow("Lena", (screen_width - width) // 2, (screen_height - height) // 2 )
    cv2.waitKey(60000)
    cv2.destroyAllWindows()