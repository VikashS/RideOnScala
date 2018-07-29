package code.vikashs.scala.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object FriendsByAge
{

  def parseLine(lines:String)={
    val fields =lines.split(",")
    val age=fields(2).toInt
    val numOfFriends=fields(3).toInt
    (age,numOfFriends)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]","FriendsByAge")
    val lines=sc.textFile("../fakefriends.csv")
    val rdd =lines.map(parseLine)
    val totalsByAge=rdd.mapValues(x => (x,1)).reduceByKey( (x,y) => (x._1 + y._1 , x._2+y._2))
    val avgValueByKey =totalsByAge.mapValues(x=> x._1 / x._2)
    val results=avgValueByKey.collect()
    results.sorted.foreach(println)
  }

}
