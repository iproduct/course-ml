from socket import herror

import cv2
import time
import tkinter as tk

width = 640
height = 480

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Success: {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('OpenCV imported')
    root = tk.Tk()
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()

    # img = cv2.imread("resources/lena.png")
    # cv2.imshow("Lena", img)
    video = cv2.VideoCapture(0)
    video.set(cv2.CAP_PROP_FRAME_WIDTH, width)
    video.set(cv2.CAP_PROP_FRAME_HEIGHT, height)
    while True:
        success, img = video.read()
        # img = cv2.resize(img, (width, height))
        cv2.imshow("Video", img)
        cv2.resizeWindow("Video", width, height)
        cv2.moveWindow("Video", (screen_width - width) // 2, (screen_height - height) // 2 )
        if cv2.waitKey(50) & 0xFF == ord('q'):
            break
