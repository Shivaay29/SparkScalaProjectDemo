import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDRepartitionExample {
  def main(args:Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
    conf.setMaster("local[5]")
    conf.setAppName("RDDRepartitionExample")
    val sc = new SparkContext(conf)
    val rdd6 = sc.parallelize(Range(1, 20))
    println("parallelize partition size :" + rdd6.partitions.size)
    val rdd1 = sc.parallelize(Range(1, 20), 6)
    println("parallelize partition num size :" + rdd1.partitions.size)
    val rddtextfile = sc.textFile("C:/Users/user/Desktop/Shared_Folder/test.txt",10)
    println("parallelize partition textfile size :" + rddtextfile.partitions.size)
    //rdd1.saveAsTextFile("C:/Users/user/Desktop/Shared_Folder/Partition")
    val rdd2=rdd1.repartition(4)
    println("parallelize re-partition size :" + rdd2.partitions.size)
    val rdd3=rdd1.coalesce(4)
    println("parallelize coalesce size :" + rdd3.partitions.size)
    val df = sc.range(0,20)
    println("Spark DataFrame partition :"+df.rdd.partitions.length)
    val df2=df.repartition(6)
    println("dataframe re-partition size :" + df2.rdd.partitions.size)
    val df3=df.coalesce(2)
    println("dataframe coalesce size :" + df3.rdd.partitions.size)
    val df4=df.groupBy("id").count()
    println(df4.rdd.getNumPartitions)
  }
}
