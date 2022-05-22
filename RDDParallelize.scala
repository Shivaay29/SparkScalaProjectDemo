import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDParallelize {
def main(args:Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf=new SparkConf()
  conf.setMaster("local[1]")
  conf.setAppName("RDDParallelize")
  val sc=new SparkContext(conf)
  val rdd= sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10,11))
  val rddcollect=rdd.collect()
  println("number of Partition :" +rdd.getNumPartitions)
  println("Action First Element :" +rdd.first())
  println("Action : RDD Converted to Array [Int] :" )
  rddcollect.foreach(println)
}
}
