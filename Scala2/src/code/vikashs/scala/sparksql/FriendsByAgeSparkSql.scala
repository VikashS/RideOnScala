package code.vikashs.scala.sparksql
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._

object FriendsByAgeSparkSql {
  
 case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def mapper(line:String): Person = {
    val fields = line.split(',')  
    //Here is the magic for case class
    //we dont need to create the Person class due to case class
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }
  
  def main(args: Array[String]) {
    
    
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
    
    val lines = spark.sparkContext.textFile("G://Workspace//ScalaWorkSpace47//fakefriends.csv")
    val people = lines.map(mapper)
    
    import spark.implicits._
    // Got the structured dataset which could be converted into table
    val schemaPeople = people.toDS
    
    schemaPeople.printSchema()
    
    schemaPeople.createOrReplaceTempView("people")
    
    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    
    val results = teenagers.collect()
    
    results.foreach(println)
    
    spark.stop()
  }
}