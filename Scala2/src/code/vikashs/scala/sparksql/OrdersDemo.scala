package code.vikashs.scala.sparksql
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._


object OrdersDemo {
  def main(args: Array[String]): Unit = {
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "OrdersDemo")
    
    val ordersRDD = sc.textFile("G://Workspace//ScalaWorkSpace47//orders")

   
    val ordersMap = ordersRDD.map(order => {(order.split(",")(0).toInt, order.split(",")(1), order.split(",")(2).toInt, order.split(",")(3))})
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
      
    import spark.implicits._
    val schemaOrderDF = ordersMap.toDF("order_id", "order_date", "order_customer_id", "order_status")
    schemaOrderDF.printSchema()
    
    schemaOrderDF.createOrReplaceTempView("orders")
    val orderDetails=spark.sql("select order_status, count(1) count_by_status from orders group by order_status").show()
    val results =orderDetails.toString()

  }
}