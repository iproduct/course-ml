import cv2
import numpy as np
from example06_img_stacking import *

def empty(arg):
    pass

if __name__ == '__main__':

    cv2.namedWindow("Trackbars")
    cv2.resizeWindow("Trackbars", 640, 320)
    cv2.createTrackbar("Hue", "Trackbars", 0, 179, empty)
