import sys
import time

import cv2 as cv


if __name__ == '__main__':
   face_cascade = cv.CascadeClassifier(cv.data.haarcascades + 'haarcascade_frontalface_default.xml')
   eye_cascade = cv.CascadeClassifier(cv.data.haarcascades + 'haarcascade_eye.xml')
   video = cv.VideoCapture(0)
   cv.namedWindow('My Video')
   if(not video.isOpened()):
      sys.exit('Could not capture video')
   start = time.time()
   while True:
      success, img = video.read()
      if not success:
         sys.exit('Could not capture a frame')
      gray = cv.cvtColor(img, cv.COLOR_BGR2GRAY)

      faces = face_cascade.detectMultiScale(
         gray,
         scaleFactor=1.1,
         minNeighbors=5,
         minSize=(30, 30)
      )

      for (x, y, w, h) in faces:
         cv.rectangle(img, (x,y), (x+w, y+h), (0, 255, 0), 2)
         roi_gray = gray[y:y+h, x:x+w]
         roi_color = img[y:y+h, x:x+w]
         eyes = eye_cascade.detectMultiScale(
            roi_gray,
            scaleFactor=1.1,
            minNeighbors=3,
            minSize=(8, 5),
            maxSize=(60, 40)
         )
         for (ex, ey, ew, eh) in eyes:
            cv.rectangle(roi_color, (ex, ey), (ex + ew, ey + eh), (255, 0, 0), 2)

      # fps = video.get(cv.CAP_PROP_FPS)
      # print("Frames per second using video.get(cv2.CAP_PROP_FPS) : {0}".format(fps))

      end = time.time()
      fps = 1 / (end - start)
      # print("Estimated frames per second : {0}".format(fps))
      cv.putText(img, 'FPR : {0}'.format(fps), (100, 30), cv.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)

      cv.imshow('My Video', img)
      # cv.imshow('Gray Video', gray)
      start = time.time()

      if cv.waitKey(20) & 0xFF == ord('q'):
         break
   video.release()
   cv.destroyAllWindows()