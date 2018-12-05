package code.vikashs.scala.sparksql.retail
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


object LowerPrice {
  def main(args: Array[String]): Unit = {
    
     val sparkConf=new SparkConf().setAppName("magicstore").setMaster("local")
     val sc: SparkContext =new SparkContext(sparkConf)
     val sqlContext=new SQLContext(sc)
      
      import sqlContext.implicits._
      import org.apache.spark.sql.functions._
      
   val compititorData=sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true").
    option("delimiter","|")
    .option("inferSchema", "true") 
    .load("G://Workspace//ScalaWorkSpace47//data//first.txt")

  val internalProductData=sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true").
    option("delimiter","|")
    .option("inferSchema", "true") 
    .load("G://Workspace//ScalaWorkSpace47//data//second.txt")


  val sellerData=sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true").
    option("delimiter","|")
    .option("inferSchema", "true") // Automatically infer data types
    .load("G://Workspace//ScalaWorkSpace47//edata//third.txt")
    
    val sellerMap: Map[Int, String] =sellerData.map(row => (row.getAs[Int]("SellerID"),row.getAs[String]("netValue"))).collect().toMap
    val sellerBroadcast=sc.broadcast(sellerMap)
  
