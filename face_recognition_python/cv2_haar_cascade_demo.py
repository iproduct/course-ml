# import numpy as np
import cv2 as cv

if __name__ == '__main__':
    faceCascade = cv.CascadeClassifier(cv.data.haarcascades + 'haarcascade_frontalface_default.xml')
    eyeCascade = cv.CascadeClassifier(cv.data.haarcascades + 'haarcascade_eye.xml')
    leftEyeCascade = cv.CascadeClassifier(cv.data.haarcascades + 'haarcascade_lefteye_2splits.xml')
    rightEyeCascade = cv.CascadeClassifier(cv.data.haarcascades + 'haarcascade_righteye_2splits.xml')
    # img = cv.imread('sachin.jpg')

    # print(cv.data.haarcascades)
    video_capture = cv.VideoCapture(0)

    while True:
        # Capture frame-by-frame
        ret, frame = video_capture.read()
        # frame = cv.imread("images/trayan.jpg")

        gray = cv.cvtColor(frame, cv.COLOR_BGR2GRAY)

        faces = faceCascade.detectMultiScale(
            gray,
            scaleFactor=1.1,
            minNeighbors=7,
            minSize=(30, 30),
            flags=cv.CASCADE_SCALE_IMAGE
        )

        # Draw a rectangle around the faces
        for (x, y, w, h) in faces:
            cv.rectangle(frame, (x, y), (x + w, y + h), (0, 255, 0), 2)
            roi_gray = gray[y:y + h, x:x + w]
            roi_color = frame[y:y + h, x:x + w]
            eyes = eyeCascade.detectMultiScale(
                roi_gray,
                scaleFactor=1.1,
                minNeighbors=3,
                maxSize=(100, 80),
                flags=cv.CASCADE_SCALE_IMAGE)
            # leftEye = leftEyeCascade.detectMultiScale(
            #     roi_gray,
            #     scaleFactor=1.1,
            #     minNeighbors=5,
            #     maxSize=(100, 80),
            #     flags=cv.CASCADE_SCALE_IMAGE)
            # rightEye = rightEyeCascade.detectMultiScale(
            #     roi_gray,
            #     scaleFactor=1.1,
            #     minNeighbors=5,
            #     maxSize=(100, 80),
            #     flags=cv.CASCADE_SCALE_IMAGE)
            for (ex, ey, ew, eh) in eyes:
                cv.rectangle(roi_color, (ex, ey), (ex + ew, ey + eh), (255, 0, 0), 2)
            # for (ex, ey, ew, eh) in leftEye:
            #     cv.rectangle(roi_color, (ex, ey), (ex + ew, ey + eh), (0, 255, 0), 2)
            # for (ex, ey, ew, eh) in rightEye:
            #     cv.rectangle(roi_color, (ex, ey), (ex + ew, ey + eh), (0, 0, 255), 2)

        # Display the resulting frame
        cv.imshow('Video', frame)

        if cv.waitKey(1) & 0xFF == 27:
            break

    # When everything is done, release the capture
    video_capture.release()
    cv.destroyAllWindows()
