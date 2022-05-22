import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ReadMultipleFiles {
def main(args:Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf=new SparkConf()
  conf.setMaster("local[*]")
  conf.setAppName("ReadMultipleFiles")
  val sc=new SparkContext(conf)
  println("Spark read Multiple textfiles into a Single RDD :")
  val rdd=sc.textFile("C:/Users/user/Desktop/Shared_Folder/spark/text01.txt,C:/Users/user/Desktop/Shared_Folder/spark/text02.txt,C:/Users/user/Desktop/Shared_Folder/spark/text03.txt,C:/Users/user/Desktop/Shared_Folder/spark/invalid.txt")
  rdd.foreach(x=> {
    println(x)
  })
  println("If you are running on a cluster you should first collect the data in order to print on a console :")
    val rdd1=sc.textFile("C:/Users/user/Desktop/Shared_Folder/spark/text01.txt")
    rdd1.collect.foreach(x=>{
      println(x)
  })
  println("WholetextFile method :")
  val rdd2=sc.wholeTextFiles("C:/Users/user/Desktop/Shared_Folder/spark/text01.txt,C:/Users/user/Desktop/Shared_Folder/spark/text02.txt,C:/Users/user/Desktop/Shared_Folder/spark/text03.txt,C:/Users/user/Desktop/Shared_Folder/spark/invalid.txt")
   rdd2.foreach(x=>{
     println(x._1+"=>"+x._2)
  })
  println("read all text files matching a pattern to single RDD :")
  val rdd3=sc.textFile("C:/Users/user/Desktop/Shared_Folder/spark/test/test01.csv")
  rdd3.foreach(x=>{
    println(x)
 })
  println("Reading multiple text files into RDD :")
  val rdd4=sc.textFile("C:/Users/user/Desktop/Shared_Folder/spark/text01.txt,C:/Users/user/Desktop/Shared_Folder/spark/text02.txt,C:/Users/user/Desktop/Shared_Folder/spark/text03.txt,C:/Users/user/Desktop/Shared_Folder/spark/invalid.txt")
  val rdd5=rdd3.map(x=>x.split(","))
  rdd5.foreach(x=>{
    println("Colm1 : "+x(0)+",Colmn2 : "+x(1))
  })
  println("Reading multiple csv files into RDD :")
  val rdd6=sc.textFile("C:/Users/user/Desktop/Shared_Folder/spark/test/test01.csv,C:/Users/user/Desktop/Shared_Folder/spark/test/test02.csv,C:/Users/user/Desktop/Shared_Folder/spark/test/test03.csv,C:/Users/user/Desktop/Shared_Folder/spark/test/test04.csv,C:/Users/user/Desktop/Shared_Folder/spark/test/invalid.csv")
  val rdd7=rdd6.map(x=>x.split(","))
  rdd7.foreach(x=>{
  println("Column1 :"+x(0)+",Column2 :"+x(1))
  })

}
}
