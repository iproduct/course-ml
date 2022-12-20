import sys
import time

import cv2 as cv

if __name__ == '__main__':
   video = cv.VideoCapture(0)
   cv.namedWindow('My Video', cv.WINDOW_NORMAL)
   cv.resizeWindow('My Video', (300,200))
   if(not video.isOpened()):
      sys.exit('Could not capture video')
   start = time.time()
   while True:
      success, img = video.read()
      if not success:
         sys.exit('Could not capture a frame')
      gray = cv.cvtColor(img, cv.COLOR_BGR2GRAY)
      edges = cv.Canny(gray,100,200)

      # fps = video.get(cv.CAP_PROP_FPS)
      # print("Frames per second using video.get(cv2.CAP_PROP_FPS) : {0}".format(fps))

      end = time.time()
      fps = 1 / (end - start)
      # print("Estimated frames per second : {0}".format(fps))
      cv.putText(img, 'FPR : {0}'.format(fps), (100, 30), cv.FONT_HERSHEY_SIMPLEX, 0.6, (0, 255, 0), 2)

      cv.imshow('My Video', img)
      cv.imshow('Gray Video', gray)
      cv.imshow('Edges', edges)
      start = time.time()

      if cv.waitKey(20) & 0xFF == ord('q'):
         break
   video.release()
   cv.destroyAllWindows()