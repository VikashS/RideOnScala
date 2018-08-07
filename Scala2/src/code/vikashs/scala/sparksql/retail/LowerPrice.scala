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
    .load("G://Workspace//ScalaWorkSpace47//ecom//ecom_competitor_data.txt")

  val internalProductData=sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true").
    option("delimiter","|")
    .option("inferSchema", "true") 
    .load("G://Workspace//ScalaWorkSpace47//ecom//internal_product_data.txtt")


  val sellerData=sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true").
    option("delimiter","|")
    .option("inferSchema", "true") // Automatically infer data types
    .load("G://Workspace//ScalaWorkSpace47//ecom//seller_data.txt")
    
    val sellerMap: Map[Int, String] =sellerData.map(row => (row.getAs[Int]("SellerID"),row.getAs[String]("netValue"))).collect().toMap
    val sellerBroadcast=sc.broadcast(sellerMap)
   /* 
    val minPriceRDD = compititorData.map(row => (row.getAs[Int]("productId"),row ) ).reduceByKey
    {
    (row1,row2) =>
      if(row1.getAs[Double]("price")<  row2.getAs[Double]("price"))
        row1
      else
        row2
  }*/

   /* val internalRDD: RDD[(Int, Row)] = internalProductData.map(row => (row.getAs[Int]("ProductId"), row))
    val rdd: RDD[(Int, (Row, Row))] = internalRDD.join(minPriceRDD)
    rdd.map {
      case (productID, (row1, row2)) =>
        val description = sellerBroadcast.value.getOrElse(row1.getAs[Int]("SellerID"), "")
        MagicPriceData(productID, row2.getAs[Double]("price"), "", row2.getAs[Double]("price"), row2.getAs[String]("rivalName"), description)
    }.toDF().show()
*/
      
   
  }

}

case class MagicPriceData(productid:Int, msprice:Double, timestamp:String, cheapestprice:Double, rivalname:String,sellerdescription:String)
