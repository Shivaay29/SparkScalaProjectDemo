import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDD {
def main(args:Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf=new SparkConf()
  conf.setMaster("local[*]")
  conf.setAppName("SparkRDD")
  val sc=new SparkContext(conf)
  println("Spark Create RDD from Seq or List (using Parallelize) :")
  val rdd=sc.parallelize(Seq(("Java,20000"),("Python,100000"),("Scala,3000")))
  rdd.foreach(println)
  println("Create an RDD from a textFile :")
  val rdd1=sc.textFile("C:/Users/user/Desktop/Shared_Folder/alice.txt")
  rdd1.foreach(x=>{
    println(x)
  })
  println("If you want to read the entire content of a file a single record use wholeTextFile :")
  val rdd2=sc.wholeTextFiles("C:/Users/user/Desktop/Shared_Folder/favmovies.csv")
  rdd2.foreach(x=>{
    println("File name :"+x._1+",Fav Movie list :"+x._2)
  })
  println("Creating from another RDD :")
  val rdd3 = rdd.map(x=>{x.split(",")})
    rdd3.foreach(x=>{
    println("Column1 :"+x(0)+",Column2 :"+x(1)+100)
  })
  println("From existing Dataframes and dataset :")
  //val myrdd=sc.range(20).toDF().rdd

}
}
