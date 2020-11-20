import cv2 as cv
import sys

WIDTH = 640
HEIGHT = 480

if __name__ == '__main__':
    print(f'OpenCV imported: {cv.version.opencv_version}')
    # img = cv.imread("resources/lena.png")
    # if img is None:
    #     sys.exit("Could not read the image.")
    # video = cv.VideoCapture("resources/mov_bbb.mp4")
    video = cv.VideoCapture(0)
    video.set(cv.CAP_PROP_FRAME_WIDTH, WIDTH)
    video.set(cv.CAP_PROP_FRAME_HEIGHT, HEIGHT)

    cv.namedWindow("Video")

    while True:
        success, img = video.read()
        if not success:
            sys.exit("Could not find video.")
        img = cv.resize(img, (WIDTH, HEIGHT))
        cv.resizeWindow("Video", WIDTH, HEIGHT)
        cv.imshow("Video", img)
        if cv.waitKey(30) & 0xFF == ord('q'):
            break;

    video.release()
    cv.destroyAllWindows()