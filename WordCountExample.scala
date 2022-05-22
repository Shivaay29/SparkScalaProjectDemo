import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object WordCountExample {
def main(args:Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf=new SparkConf()
  conf.setMaster("local[3]")
  conf.setAppName("WordCountExample")
    val sc=new SparkContext(conf)
    val rdd = sc.textFile("C:/Users/user/Desktop/Shared_Folder/Hello.csv")
  println("initial partition NumPartitions count :"+rdd.getNumPartitions)
  val repartrdd = rdd.repartition(5)
  println("Re-partition NumPartitions count :"+repartrdd.getNumPartitions)
  //rdd coalesce
  val coelesrdd = rdd.coalesce(2)
  println("After Coalesce NumPartitions count :"+coelesrdd.getNumPartitions)
  rdd.collect().foreach(println)
  // rdd flatmap transformation
  val rdd2=rdd.flatMap(x=>x.split(" "))
  println("flatmap function :")
    rdd2.foreach(x=>println(x))
  // creating tuple by adding 1 to each word
  val rdd3=rdd2.map(x=>(x,1))
  println("map function :")
  rdd3.foreach(println)
  // filter transformation
  val rdd4=rdd3.filter(x=>x._1.startsWith("B"))
  println("filter transformation transformation :")
  rdd4.foreach(println)
  //reducebykey transformation
  val rdd5=rdd3.reduceByKey(_+_)
  println("reducebykey transformation :")
  rdd5.foreach(println)
  //swapwords,count and sortByKey transformation
  val rdd6=rdd5.map(x=>(x._2,x._1)).sortByKey()
  println("rdd6 swapwords transformation :")
  rdd6.foreach(println)
  //Action - count
  println("Count :"+rdd6.count())
  println("initial partition NumPartitions count :"+rdd6.getNumPartitions)
  //Action - first
  val firstrec=rdd6.first()
  println("firstrec :" +firstrec._1+","+firstrec._2)
  //Action - Max
  val maxrec=rdd6.max()
  println("Maxrec :" +maxrec._1+","+maxrec._2)
  //Action - Reduce
  val totalwordcount=rdd6.reduce((a,b)=>(a._1+b._1,a._2))
  println("Data reduce count :" +totalwordcount)
  //Action - take
  val takedata=rdd6.take(3)
  takedata.foreach(x=>println("Take Top 3 keys :"+x._1+",Take Top 3 value :"+x._2))
  //Action - collect
  val data=rdd6.collect()
  data.foreach(x=>println("Key :"+x._1+", value :"+x._2))


}
}
