import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Pehladf {
def main(args:Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf=new SparkConf()
  conf.setMaster("local[*]")
  conf.setAppName("Pehladf")
  val sc=new SparkContext(conf)
  //val pehlardd=sc.parallelize(1 to 10).map(x=>x,"DF ka Data")
  //val pehladf=pehlardd.toDF("ID","Column Header")
  //pehladf.printSchema()

}
}
