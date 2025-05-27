## Spark & Pyspark Installation and Configuration

`Python 3.10` is required (tested to work, `3.12` gives problem with `distutils` package)
`PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` environment variables should be set to 
virtual environment python.exe script - e.g. 
`D:\CourseDML\git\course-ml\fmi-2025-10-pyspark-mlib\.venv\Scripts\python.exe`

`Spark 3.5.5` should be unzipped in a directory of choice 
and `SPARK_HOME` environment variable should be set to this directory

## Installing PySpark

`pip install pyspark===3.5.5`

`pip install numpy`

## Running Spark Standalone in Windows
spark-class org.apache.spark.deploy.master.Master
spark-class org.apache.spark.deploy.master.Master spark://ip:port 
spark-class org.apache.spark.deploy.master.Master spark://10.108.5.3:7077

spark-class org.apache.spark.deploy.worker.Worker spark://10.108.5.3:7077




