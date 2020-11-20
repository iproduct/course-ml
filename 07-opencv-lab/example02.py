import cv2 as cv
import sys

if __name__ == '__main__':
    print(f'OpenCV imported: {cv.version.opencv_version}')
    # img = cv.imread("resources/lena.png")
    # if img is None:
    #     sys.exit("Could not read the image.")
    video = cv.VideoCapture("resources/mov_bbb.mp4")

    while True:
        success, img = video.read()
        if not success:
            sys.exit("Could not find video.")
        cv.imshow("Video", img)
        if cv.waitKey(30) & 0xFF == ord('q'):
            break;

    cv.destroyAllWindows()