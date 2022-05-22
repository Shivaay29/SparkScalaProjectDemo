import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object emptyRDD {
def main(args:Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf=new SparkConf()
  conf.setMaster("local[3]")
  conf.setAppName("emptyRDD")
  val sc=new SparkContext(conf)
 val rdd=sc.emptyRDD   // creates EmptyRDD[0]
 val rddstring=sc.emptyRDD[String]  // creates EmptyRDD[1]
 println(rdd)
 println(rddstring)
 println("Number of Partition :"+rdd.getNumPartitions)  // returns 0 partition
  // rdd.saveAsTextFile("C:/Users/user/IdeaProjects/SparkScalaProjectDemo/src/main/test.txt")
  val rdd2=sc.parallelize(Seq.empty[String])
  println("rdd2 value is :"+rdd2)
  println("Number of Partition :"+rdd2.getNumPartitions)
  val rdd3=sc.parallelize(Seq(""))
  println("rdd2 value is :"+rdd3)
  type pairrdd=(String,Int)
  val resultrdd=sc.emptyRDD[pairrdd]
  println("resultrdd value is :"+resultrdd)
}
}
