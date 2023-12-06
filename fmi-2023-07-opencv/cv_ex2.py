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
            gray = cv.cvtColor(frame, cv.COLOR_BGR2GRAY)
            faces = faceCascade.detectMultiScale(gray, 1.3, 5)
            for (x,y,w,h) in faces:
                frame = cv.rectangle(frame, (x,y), (x+w, y+h), (255, 0, 0), 2)
                roi_gray = gray[y:y + h, x: x + w]
                eyes = eyesCascade.detectMultiScale(roi_gray, scaleFactor=1.1, minNeighbors=3,
                                                    maxSize=(50, 40), flags=cv.CASCADE_SCALE_IMAGE)
                for(ex, ey, ew, eh) in eyes[:2]:
                    cv.rectangle(frame, (x + ex, y + ey), (x + ex + ew, y + ey + eh), (0, 0, 255), 2)
            cv.imshow("Video", frame)
    else:
        print("Error opening video file.")
    cap.release()
    cv.destroyAllWindows()