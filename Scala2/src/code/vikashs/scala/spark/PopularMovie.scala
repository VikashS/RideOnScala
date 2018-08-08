package code.vikashs.scala.spark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object PopularMovie {
  
  //userID,movieId,rating,timestamp
  //196	242	3	881250949
 
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "PopularMovie")
    val lines = sc.textFile("G://Workspace//ScalaWorkSpace47//movie.data")
    val rddMovies = lines.map(x=>  (x.split("\t")(1).toInt,1))
    //(242,881250949)
    val filteredMovie=rddMovies.reduceByKey((x,y)=> x+y)
    val flipped = filteredMovie.map( x => (x._2, x._1) )
    val sortedMovies = flipped.sortByKey()
    val result=sortedMovies.collect()
    result.foreach(println)
    
  }
  
}