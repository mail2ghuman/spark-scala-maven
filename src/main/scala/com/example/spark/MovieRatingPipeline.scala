package com.example.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * MovieRatingPipeline reads Rotten Tomatoes movie data, categorizes each movie
 * based on its tomatometer_rating, and saves a summary to a Parquet table.
 *
 * Rating categories (based on tomatometer_rating):
 *   Bad:   rating < 60
 *   Good:  rating >= 60 and < 80
 *   Great: rating >= 80
 *
 * Output table: movie_ratings (Parquet)
 *   category: String  — "Bad", "Good", or "Great"
 *   count:    Long    — number of movies in that category
 *   movies:   String  — all movie titles in the category, separated by ", "
 */
object MovieRatingPipeline {

  val BAD_THRESHOLD = 60
  val GOOD_THRESHOLD = 80

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Movie Rating Pipeline")
      .master("local[*]")
      .getOrCreate()

    val inputPath = if (args.length > 0) args(0) else "Rotten Tomatoes Movies.csv"
    val outputPath = if (args.length > 1) args(1) else "movie_ratings"

    println("=== Movie Rating Pipeline ===")
    println(s"Input path:  $inputPath")
    println(s"Output path: $outputPath")

    val movies = readMovies(spark, inputPath)
    val categorized = categorizeMovies(movies)
    val summary = aggregateByCategory(categorized)

    println("\n--- Movie Rating Summary ---")
    summary.show(truncate = false)

    saveToParquet(summary, outputPath)
    println(s"\nMovie ratings written to: $outputPath")

    spark.stop()
    println("Movie Rating Pipeline completed successfully.")
  }

  /** Read the Rotten Tomatoes CSV, selecting only needed columns. */
  def readMovies(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .option("escape", "\"")
      .csv(path)
      .select(
        col("movie_title"),
        col("tomatometer_rating").cast(IntegerType).as("tomatometer_rating")
      )
      .filter(col("tomatometer_rating").isNotNull)
  }

  /**
   * Add a "category" column based on tomatometer_rating:
   *   Bad:   rating < 60
   *   Good:  rating >= 60 and < 80
   *   Great: rating >= 80
   */
  def categorizeMovies(movies: DataFrame): DataFrame = {
    movies.withColumn("category",
      when(col("tomatometer_rating") < BAD_THRESHOLD, "Bad")
        .when(col("tomatometer_rating") < GOOD_THRESHOLD, "Good")
        .otherwise("Great")
    )
  }

  /**
   * Aggregate by category: count movies and collect all titles into a
   * comma-separated string.
   */
  def aggregateByCategory(categorized: DataFrame): DataFrame = {
    categorized.groupBy("category")
      .agg(
        count("movie_title").as("count"),
        concat_ws(", ", collect_list("movie_title")).as("movies")
      )
      .orderBy("category")
  }

  /** Save the summary DataFrame as a Parquet table. */
  def saveToParquet(summary: DataFrame, outputPath: String): Unit = {
    summary.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }
}
