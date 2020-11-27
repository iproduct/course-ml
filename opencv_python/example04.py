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

    resized = cv2.resize(img, (1024,1024), 0)
    cropped = resized[100:612, 100:612]
    print(img.shape)
    cv2.imshow("Lena", resized)
    cv2.imshow("Lena [cropped]", cropped)


    # cv2.resizeWindow("Lena", width, height)
    # cv2.moveWindow("Lena", (screen_width - width) // 2, (screen_height - height) // 2 )
    cv2.waitKey(60000)
    cv2.destroyAllWindows()