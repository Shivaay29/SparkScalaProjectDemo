import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
object Union {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Union")
    val sc=new SparkContext(conf)
    val rdd1=sc.parallelize(Array(1,2,3,4,5,6))
    val rdd2=sc.parallelize(Array(5,6,7,8,9,10))
    val unionrdd=rdd1.union(rdd2)
    val unionrdd1=rdd1.union(rdd2).count()
    unionrdd.collect().foreach(println)
    println(s"unionrdd1 value is :$unionrdd1")
    val interrdd=rdd1.intersection(rdd2)
    val interrdd1=rdd1.intersection(rdd2).count()
    interrdd.collect().foreach(println)
    println(s"interrdd value is :$interrdd1")
    val subtractrdd=rdd1.subtract(rdd2)
    val subtractrdd1=rdd1.subtract(rdd2).count()
    subtractrdd.collect().foreach(println)
    println(s"subtractrdd value is :$subtractrdd1")
  }
}