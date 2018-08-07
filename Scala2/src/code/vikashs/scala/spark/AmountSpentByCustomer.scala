package code.vikashs.scala.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object AmountSpentByCustomer {

   // 44,8602,37.19
    //35,5368,65.89
  def parseLine(lines: String) = {
    val fields = lines.split(",")
    val customerId = fields(0).toInt
    val numOfFriends = fields(2).toFloat
    (customerId, numOfFriends)
  }
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "AmountSpentByCustomer")
    val lines = sc.textFile("G://Workspace//ScalaWorkSpace47//orderDetails.csv")
   
    //(44,37.19) & (44,12.00)
    val rddCust = lines.map(parseLine)
     val totalByCustomer = rddCust.reduceByKey( (x,y) => x + y )
     
     val unsorted = totalByCustomer.map( x => (x._2, x._1) )
    
    val totalByCustomerSorted = unsorted.sortByKey()
    
    val results = totalByCustomerSorted.collect()
    
    results.foreach(println)
  }

}