import cv2

if __name__ == "__main__":
    cap = cv2.VideoCapture(0)
    if cap.isOpened():
        print('Video successfully opened!')
        while cv2.waitKey(10) != ord('q'):
            ret, frame = cap.read()
            if ret == False:
                print('Error opening video stream or file')
                break
            cv2.imshow('Test.MP4', frame)
    cap.release()
    cv2.destroyAllWindows()
