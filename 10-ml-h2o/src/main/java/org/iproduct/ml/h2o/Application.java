package org.iproduct.ml.h2o;

import hex.genmodel.easy.exception.PredictException;
import nu.pattern.OpenCV;
import org.iproduct.ml.h2o.domain.Resource;
import org.iproduct.ml.h2o.services.ExtractEmbeddingsService;
import org.opencv.core.Core;
import org.opencv.dnn.Dnn;
import org.opencv.dnn.Net;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        // load opencv native library
        // System.loadLibrary("opencv_java450-2");
        OpenCV.loadShared();
//        String protoPath = "src/main/resources/face_detection_model/deploy.prototxt";
//        String modelPath = "src/main/resources/face_detection_model/res10_300x300_ssd_iter_140000.caffemodel";
//        Net detector = Dnn.readNetFromCaffe(protoPath, modelPath);
//        ExtractEmbeddingsService ees = new ExtractEmbeddingsService();
//        ees.init();
//        try {
//            ees.extractEmbeddings(new Resource());
//        } catch (PredictException e) {
//            e.printStackTrace();
//        }
//        System.out.println("Ready");
        SpringApplication.run(Application.class, args);
    }

}
