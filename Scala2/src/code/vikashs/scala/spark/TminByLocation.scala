package code.vikashs.scala.spark

import org.apache.spark.SparkContext
import scala.math._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object TminByLocation 
{
  def main(args: Array[String]): Unit = 
  {
     val sc = new SparkContext("local[*]","TminByLocation")
     val lines=sc.textFile("G://Workspace//ScalaWorkSpace47//tempra.csv")
     val parsedLines =lines.map(parsedLine)
     val tminFiltered=parsedLines.filter(x=> x._2=="TMIN")
     val stationTemp= tminFiltered.map(x=> (x._1,x._3.toFloat))
     val minTempByStation = stationTemp.reduceByKey( (x,y) => min(x,y))
     val results=minTempByStation.collect()
     for (result <- results.sorted) {
       val station = result._1
       val temp = result._2
       val formattedTemp = f"$temp%.2f F"
       println(s"$station minimum temperature: $formattedTemp") 
    }

  }
  //EZE00100082,18000101,TMAX,-86,,,E,
  //EZE00100082,18000101,TMIN,-135,,,E,
  def parsedLine(lines:String)={
    val fields =lines.split(",")
    val stationid=fields(0)
    val entryType=fields(2)
    val temparature=fields(3).toFloat * 0.1f * (9.0f /5.0f) + 32.0f
    (stationid,entryType,temparature)
    
  }
}