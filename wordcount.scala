import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object wordcount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Sparktest")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("C:/Users/user/Desktop/Shared_Folder/ABC.csv")
    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
    counts.collect().foreach(println)
  }
}