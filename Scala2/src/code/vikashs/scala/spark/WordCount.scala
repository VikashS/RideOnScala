package code.vikashs.scala.spark
import org.apache.spark._

object WordCount 
{
  def main(args: Array[String]): Unit = {
      val sc = new SparkContext("local[*]","TminByLocation")
      val lines=sc.textFile("G://Workspace//ScalaWorkSpace47//book.txt")
      
      val dataMap=lines.flatMap(x => x.split(" "))
      
      val wordCounts=dataMap.countByValue()
      wordCounts.foreach(println)
  }
}