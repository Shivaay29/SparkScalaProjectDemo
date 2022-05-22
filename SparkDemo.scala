import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
object SparkDemo {
def main(args:Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.ERROR)
val conf=new SparkConf().setMaster("local[*]").setAppName("SparkDemo")
  val sc=new SparkContext(conf)
  val rdd1=sc.parallelize(Array(1,2,3,4))
   println(rdd1.reduce(_+_))
  rdd1.collect().foreach(println)
  val map=rdd1.map(x=>x.to(6))
  map.collect().foreach(println)
}
}