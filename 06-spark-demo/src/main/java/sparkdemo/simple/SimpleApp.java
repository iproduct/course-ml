package sparkdemo.simple;

/* SimpleApp.java */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SimpleApp {
    public static void main(String[] args) {
        String inputFile = "README.md"; // Should be some file on your system
        SparkSession spark = SparkSession.builder()
//                .master("local")
                .master("spark://192.168.0.12:7077")
                .appName("Simple Application").getOrCreate();
        Dataset<String> data = spark.read().textFile(inputFile).cache();
        System.out.printf("Count Lines: %d", data.count());

//        long numAs = data.filter((FilterFunction<String>)  s -> s.contains("Spark")).count();
//        long numBs = data.filter((FilterFunction<String>) s -> s.contains("Python")).count();
//
//        System.out.println("Lines with Spark: " + numAs + ", lines with Python: " + numBs);

        spark.stop();
    }
}
