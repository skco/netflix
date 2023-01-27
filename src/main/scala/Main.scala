import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DateType, DoubleType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.countDistinct

object App {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("netflix")
      .master("local")
      .getOrCreate()

      var netflixDF:DataFrame = spark.read
      .option("header", true)
      .option("delimiter", ",")
      .csv("netflix_titles.csv")

       netflixDF = netflixDF.na.fill("NULL")

      //count all
       println("count:",netflixDF.count())

       //count by type
       netflixDF.groupBy("type").count().show()

       // count by director
       netflixDF
         .groupBy("director")
         .count()
         .sort(col("count").desc)
         .show(numRows= netflixDF.count().toInt) //show all rows

       //count by release_year
       netflixDF
         .groupBy("release_year")
         .count()
         .sort(col("release_year").desc)
         .show(numRows = netflixDF.count().toInt)  //show all rows



       val netflixExplodedOninListedDF: DataFrame= netflixDF
         .withColumn("listed_in", split(col("listed_in"), ","))
         .select(col("show_id"), explode(col("listed_in")))

       netflixExplodedOninListedDF.show(truncate = false,numRows = netflixExplodedOninListedDF.count().toInt)
       netflixExplodedOninListedDF.groupBy("col").count().show()

       netflixExplodedOninListedDF.printSchema()



  }
}
