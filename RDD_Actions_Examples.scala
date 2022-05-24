import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable._
object RDD_Actions_Examples {
def main(args:Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf=new SparkConf()
  conf.setMaster("local[*]")
  conf.setAppName("RDD_Actions_Examples")
  val sc=new SparkContext(conf)
  val inputrdd=sc.parallelize(List(("Z",1),("A",20),("B",30),("C",40),("B",30),("K",60)))
  val listrdd=sc.parallelize(List(1,2,3,4,5,3,2),2)
val data:Array[Int]=listrdd.collect()
  data.foreach(println)
  def param0=(accu:Int,v:Int)=>accu+v
  def param1=(accu1:Int,accu2:Int)=>accu1+accu2
  println("ListRDD Aggregate :"+listrdd.aggregate(0)(param0,param1))
  def param2=(accu:Int,v:(String,Int))=>accu+v._2
  def param3=(accu1:Int,accu2:Int)=>accu1+accu2
  println("Inputrdd Aggregate :"+inputrdd.aggregate(0)(param2,param3))
  def param4=(acc:Int,v:Int)=>acc+v
  def param5=(acc1:Int,acc2:Int)=>acc1+acc2
  println("tree Aggregate :"+listrdd.treeAggregate(0)(param4,param5))
  // fold
  println("fold ListRdd:"+listrdd.fold(0) {(acc,v)=>
    val sum=acc+v
    sum
  })
  println("fold Inputrdd :"+inputrdd.fold("Total",0){(accu:(String,Int),v:(String,Int))=>val sum = accu._2+v._2
    ("Total",sum)
  })
  //reduce
  println("reduce :"+listrdd.reduce(_+_))
  println("reduce alternate :"+listrdd.reduce((a,b)=>a+b))
  println("reduce Inputrdd :"+inputrdd.reduce((x,y)=>("Total",x._2+y._2)))
  //tree reduce : This is similar to reduce
  println("Tree Reduce :"+listrdd.treeReduce(_+_))
  //count, countApprox,countApproxDistinct
  println("Count :"+listrdd.count())
  println("CountApprox :"+listrdd.countApprox(1200))
  println("CountApproxDistinct :"+listrdd.countApproxDistinct())
  println("CountApproxDistinct :"+inputrdd.countApproxDistinct())
  //countByvalue, countByValueApprox
  println("CountByvalue :"+listrdd.countByValue())
  println("CountByValueApprox :"+listrdd.countByValueApprox(1200))
  //first
  println("first list rdd value :"+listrdd.first())
  println("first input list rdd value :"+inputrdd.first())
  //top
  println("top list rdd value :"+listrdd.top(3).mkString(","))
  println("top input list rdd value :"+inputrdd.top(3).mkString(","))
  //max
  println("max list rdd value :"+listrdd.max())
  println("max input list rdd value :"+inputrdd.max())
  //min
  println("min list rdd value :"+listrdd.min())
  println("min input list rdd value :"+inputrdd.min())
  //take
  println("take list rdd value :"+listrdd.take(2).mkString(","))
  println("take ordered list rdd value :"+listrdd.takeOrdered(2).mkString(","))
  println("take sample list rdd value :"+listrdd.takeSample(true,2,2).mkString(","))
  println("take input list rdd value :"+inputrdd.take(2).mkString(","))
}
}
