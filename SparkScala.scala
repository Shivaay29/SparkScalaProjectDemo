import org.apache.spark.sql.SparkSession
  object SparkScala {
    def main(args: Array[String]): Unit = {
      val logFile = "C:\\Users\\user\\IdeaProjects\\SparkScalaProjectDemo\\src\\main\\scala\\README.md" // Should be some file on your system
      val spark = SparkSession.builder.appName("Simple Application").master(master= "local[4]").getOrCreate()
      val logData = spark.read.textFile(logFile).cache()
      val numAs = logData.filter(line => line.contains("a")).count()
      val numBs = logData.filter(line => line.contains("b")).count()
      println(s"Lines with a: $numAs, Lines with b: $numBs")
      spark.stop()
    }

}
