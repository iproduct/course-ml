import sys
import time
import numpy as np

import cv2 as cv
prototxt = 'face_detection_model/deploy.prototxt'
caffemodel = 'face_detection_model/res10_300x300_ssd_iter_140000.caffemodel'

if __name__ == '__main__':
   print("[INFO] loading model...")
   net = cv.dnn.readNetFromCaffe(prototxt, caffemodel)
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

      # grab the frame dimensions and convert it to a blob
      (h, w) = img.shape[:2]
      blob = cv.dnn.blobFromImage(cv.resize(img, (300, 300)), 1.0, (300, 300), (104.0, 177.0, 123.0))

      # pass the blob through the network and obtain the detections and predictions
      net.setInput(blob)
      detections = net.forward()
      count = 0

      # loop over the detections
      for i in range(0, detections.shape[2]):
         # extract the confidence (i.e., probability) associated with the
         # prediction
         confidence = detections[0, 0, i, 2]
         # print(confidence * 100)

         # filter out weak detections by ensuring the `confidence` is
         # greater than the minimum confidence
         if confidence < 0.5:
            continue
         count += 1
         # compute the (x, y)-coordinates of the bounding box for the object
         box = detections[0, 0, i, 3:7] * np.array([w, h, w, h])
         (startX, startY, endX, endY) = box.astype("int")

         # draw the bounding box of the face along with the associated probability
         text = "{:.2f}%".format(confidence * 100) + ", Count " + str(count)
         y = startY - 10 if startY - 10 > 10 else startY + 10
         cv.rectangle(img, (startX, startY), (endX, endY), (0, 255, 0), 2)
         cv.putText(img, text, (startX, y), cv.FONT_HERSHEY_SIMPLEX, 0.45, (0, 255, 0), 2)

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