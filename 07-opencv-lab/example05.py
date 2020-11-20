import cv2 as cv
import sys
import numpy as np

# mouse callback function
def draw_circle(event,x,y,flags,param):
    if event == cv.EVENT_LBUTTONDOWN:
        cv.circle(img,(x,y),3,(0,0,255),-1)

# img = np.zeros((512,512,3), np.uint8)

if __name__ == '__main__':
    print(f'OpenCV imported: {cv.version.opencv_version}')
    img = cv.imread("resources/cards.jpg")
    if img is None:
        sys.exit("Could not read the image.")
    print(img.shape)
    cv.namedWindow('Image')
    cv.setMouseCallback('Image', draw_circle)
    # cropped = img[200:400, 100:250]

    events = [i for i in dir(cv) if 'EVENT' in i]
    print(events)
    while True:
        cv.imshow('Image', img)
        if cv.waitKey(20) & 0xFF == 27:
            break

    cv.destroyAllWindows()
