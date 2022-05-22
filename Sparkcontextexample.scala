import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Sparkcontextexample {
def main(args:Array[String]): Unit = {
 Logger.getLogger("org").setLevel(Level.ERROR)
  val conf=new SparkConf().setMaster("local[*]").setAppName("Sparkcontext")
  val sc=new SparkContext(conf)
  val rdd=sc.textFile("C:/Users/user/Desktop/Shared_Folder/alice.txt")
  println("first SparkContext :")
  println("App Name :" +sc.appName)
  println("Deploy Mode :" +sc.deployMode)
  println("Master :" +sc.master)

}
}
