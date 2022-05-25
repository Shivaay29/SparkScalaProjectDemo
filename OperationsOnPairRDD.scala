import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable._

object OperationsOnPairRDD {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args:Array[String]): Unit = {
    val conf=new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("OperationsOnPairRDD")
    val sc=new SparkContext(conf)
    val rdd=sc.parallelize(List("Germany India USA","USA India Russia","India Brazil Canada China"))
    val flatrdd=rdd.flatMap(x=>x.split(" "))
    val pairrdd=flatrdd.map(x=>(x,1))
    println("count of the Element by map function :")
    pairrdd.foreach(println)
    println("distinct value of the Element by distinct function :")
    pairrdd.distinct().foreach(println)
    // sort By key
    println("Sort By Key ==>")
    val sortrdd=pairrdd.sortByKey()
    sortrdd.collect.foreach(println)
    // reduce By key
    println("Reduce By Key ==>")
    val wordcount=pairrdd.reduceByKey(_+_)
    wordcount.foreach(println)
    def param0=(accu:Int,v:Int)=>accu+v
    def param1=(accu1:Int,accu2:Int)=>accu1+accu2
      val wordcount2=pairrdd.aggregateByKey(0)(param0,param1)
    println("Aggregate By Key ==>")
    wordcount2.foreach(println)
    //Keys
    println("Keys ==>")
    wordcount2.keys.foreach(println)
    //values
    println("Values ==>")
    wordcount2.values.foreach(println)
    //counts
    println("Count of the elements ==>"+wordcount2.count())
    //Collect As Map
    println("Collect As Map Values ==>")
    pairrdd.collectAsMap().foreach(println)
  }

}
