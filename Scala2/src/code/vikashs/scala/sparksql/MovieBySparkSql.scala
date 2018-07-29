package code.vikashs.scala.sparksql
import org.apache.spark._
import org.apache.spark.SparkContext._

object MovieBySparkSql {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MovieBySparkSql")

    // Read in each rating line
    val lines = sc.textFile("G://Workspace//ScalaWorkSpace47//movie.data")

    // Map to (movieID, 1) tuples
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))

    // Count up all the 1's for each movie
    val movieCounts = movies.reduceByKey((x, y) => x + y)

    // Flip (movieID, count) to (count, movieID)
    val flipped = movieCounts.map(x => (x._2, x._1))

    // Sort
    val sortedMovies = flipped.sortByKey()

    // Collect and print results
    val results = sortedMovies.collect()

    results.foreach(println)
  }

}
