spark-class org.apache.spark.deploy.master.Master
spark-class org.apache.spark.deploy.master.Master spark://ip:port 
spark-class org.apache.spark.deploy.master.Master spark://10.108.5.3:7077

spark-class org.apache.spark.deploy.worker.Worker spark://10.108.5.3:7077

JVM Options: --add-exports java.base/sun.nio.ch=ALL-UNNAMED
