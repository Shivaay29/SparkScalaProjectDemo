import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object List {
  def apply(i: Int, i1: Int, i2: Int, i3: Int, i4: Int): scala.Seq[Int] = ???

  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
 val conf=new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("List")
    val sc=new SparkContext(conf)
    val text1=sc.textFile("C:/Users/user/Desktop/Shared_Folder/bdf_database.txt")
    val count=text1.flatMap(line=>line.split(" "))
    val mapf=count.map(word=>(word,1))
    val reducef=mapf.reduceByKey(_+_)
    reducef.collect().foreach(println)
}
}
