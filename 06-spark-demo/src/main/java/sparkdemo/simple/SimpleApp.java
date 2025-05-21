package sparkdemo.simple;

/* SimpleApp.java */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SimpleApp {
    public static void main(String[] args) {
        String inputFile = "README.md"; // Should be some file on your system
        SparkConf sconf = new SparkConf()
//                .set("spark.eventLog.dir", "hdfs://nn:8020/user/spark/applicationHistory")
//                .set("spark.eventLog.enabled", "true")
                .setJars(new String[]{"D:\\CourseDML\\git\\course-ml\\06-spark-demo\\build\\libs\\06-spark-demo-1.0-SNAPSHOT.jar"})
                .setMaster("spark://10.108.5.3:7077");
        SparkSession spark = SparkSession.builder()
                .config(sconf)
//                .master("local")
//                .master("spark://10.108.5.3:7077")
                .appName("Simple Application").getOrCreate();
        Dataset<String> data = spark.read().textFile(inputFile).cache();
        System.out.printf("\n!!!!!!!!!!!! Count Lines: %d\n\n", data.count());

        long numAs = data.filter((FilterFunction<String>)  s -> s.contains("Spark")).count();
        long numBs = data.filter((FilterFunction<String>) s -> s.contains("Python")).count();

        System.out.println("Lines with Spark: " + numAs + ", lines with Python: " + numBs);

//        spark.stop();
    }
}
