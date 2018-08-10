package code.vikashs.scala.spark
import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.log4j._

object PopularMovieBroadCast 
{
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames(): Map[Int, String] = {

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("G://Workspace//ScalaWorkSpace47//movie.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }

  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMoviesNicer")

    // Create a broadcast variable of our ID -> movie name map
    var nameDict = sc.broadcast(loadMovieNames)

    // Read in each rating line
    val lines = sc.textFile("G://Workspace//ScalaWorkSpace47//movie.data")

    // Map to (movieID, 1) tuples
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))

    // Count up all the 1's for each movie
    val movieCounts = movies.reduceByKey((x, y) => x + y)

    // swap (movieID, count) to (count, movieID)
    val flipped = movieCounts.map(x => (x._2, x._1))

    // Sorted the value
    val sortedMovies = flipped.sortByKey()

    // Fold in the movie names from the broadcast variable
    val sortedMoviesWithNames = sortedMovies.map(x => (nameDict.value(x._2), x._1))

    // Collect and print results
    val results = sortedMoviesWithNames.collect()

    results.foreach(println)
  }

}