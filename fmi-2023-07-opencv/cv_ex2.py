import cv2 as cv

if __name__ == '__main__':


    # read a camera video stream
    cap = cv.VideoCapture(0)
    # load Haar cascade face and eye detectors
    faceCascade = cv.CascadeClassifier(cv.data.haarcascades + 'haarcascade_frontalface_default.xml')
    eyesCascade = cv.CascadeClassifier(cv.data.haarcascades + 'haarcascade_eye.xml')
    if cap.isOpened():
        print("Video opened successfully")
        cap.set(cv.CAP_PROP_FRAME_WIDTH, 1280)
        cap.set(cv.CAP_PROP_FRAME_HEIGHT, 720)
        ret, frame = cap.read()
        print(frame.shape)
        while cv.waitKey(1) != ord('q'):
            ret, frame = cap.read()
            if ret == False:
                print("Video end")
                break
            # frame = cv.resize(frame, (1024, 768))
            frame = cv.resize(frame, None, fx=0.5, fy=0.5)
            cv.imshow("Video", frame)
            gray = cv.cvtColor(frame, cv.COLOR_BGR2GRAY)
            cv.imshow("Video Grayscale", gray)
    else:
        print("Error opening video file.")
    cap.release()
    cv.destroyAllWindows()