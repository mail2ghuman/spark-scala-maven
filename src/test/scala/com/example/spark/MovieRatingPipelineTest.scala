package com.example.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class MovieRatingPipelineTest extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder()
    .appName("MovieRatingPipelineTest")
    .master("local[*]")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("readMovies should load CSV and select movie_title and tomatometer_rating") {
    val path = getClass.getClassLoader.getResource("sample-data/rotten_tomatoes_sample.csv").getPath
    val df = MovieRatingPipeline.readMovies(spark, path)

    assert(df.columns.toSet === Set("movie_title", "tomatometer_rating"))
    // Only rows with non-null tomatometer_rating should be included
    assert(df.count() === 5)
  }

  test("categorizeMovies should assign Bad, Good, and Great categories") {
    import spark.implicits._

    val movies = Seq(
      ("Movie A", 30),
      ("Movie B", 50),
      ("Movie C", 90),
      ("Movie D", 39),
      ("Movie E", 70)
    ).toDF("movie_title", "tomatometer_rating")

    val categorized = MovieRatingPipeline.categorizeMovies(movies)

    assert(categorized.columns.contains("category"))

    val movieA = categorized.filter($"movie_title" === "Movie A").first()
    assert(movieA.getAs[String]("category") === "Bad")

    val movieB = categorized.filter($"movie_title" === "Movie B").first()
    assert(movieB.getAs[String]("category") === "Good")

    val movieC = categorized.filter($"movie_title" === "Movie C").first()
    assert(movieC.getAs[String]("category") === "Great")

    val movieD = categorized.filter($"movie_title" === "Movie D").first()
    assert(movieD.getAs[String]("category") === "Bad")

    val movieE = categorized.filter($"movie_title" === "Movie E").first()
    assert(movieE.getAs[String]("category") === "Great")
  }

  test("categorizeMovies boundary: rating 40 should be Good, 69 should be Good, 70 should be Great") {
    import spark.implicits._

    val movies = Seq(
      ("Boundary Low", 40),
      ("Boundary Mid", 69),
      ("Boundary High", 70)
    ).toDF("movie_title", "tomatometer_rating")

    val categorized = MovieRatingPipeline.categorizeMovies(movies)

    val low = categorized.filter($"movie_title" === "Boundary Low").first()
    assert(low.getAs[String]("category") === "Good")

    val mid = categorized.filter($"movie_title" === "Boundary Mid").first()
    assert(mid.getAs[String]("category") === "Good")

    val high = categorized.filter($"movie_title" === "Boundary High").first()
    assert(high.getAs[String]("category") === "Great")
  }

  test("aggregateByCategory should produce category, count, and movies columns") {
    import spark.implicits._

    val movies = Seq(
      ("Movie A", 50),
      ("Movie B", 75),
      ("Movie C", 90),
      ("Movie D", 30),
      ("Movie E", 85)
    ).toDF("movie_title", "tomatometer_rating")

    val categorized = MovieRatingPipeline.categorizeMovies(movies)
    val summary = MovieRatingPipeline.aggregateByCategory(categorized)

    assert(summary.columns.toSet === Set("category", "count", "movies"))
    assert(summary.count() === 3)

    val bad = summary.filter($"category" === "Bad").first()
    assert(bad.getAs[Long]("count") === 1)
    val badMovies = bad.getAs[String]("movies")
    assert(badMovies.contains("Movie D"))

    val good = summary.filter($"category" === "Good").first()
    assert(good.getAs[Long]("count") === 1)
    assert(good.getAs[String]("movies").contains("Movie A"))

    val great = summary.filter($"category" === "Great").first()
    assert(great.getAs[Long]("count") === 3)
    val greatMovies = great.getAs[String]("movies")
    assert(greatMovies.contains("Movie B"))
    assert(greatMovies.contains("Movie C"))
    assert(greatMovies.contains("Movie E"))
  }

  test("saveToParquet should write data that can be read back") {
    import spark.implicits._

    val movies = Seq(
      ("Movie A", 30),
      ("Movie B", 50),
      ("Movie C", 90)
    ).toDF("movie_title", "tomatometer_rating")

    val categorized = MovieRatingPipeline.categorizeMovies(movies)
    val summary = MovieRatingPipeline.aggregateByCategory(categorized)

    val outputPath = s"/tmp/movie_ratings_test_${System.currentTimeMillis()}"

    MovieRatingPipeline.saveToParquet(summary, outputPath)

    val loaded = spark.read.parquet(outputPath)
    assert(loaded.count() === 3)
    assert(loaded.columns.toSet === Set("category", "count", "movies"))

    // Clean up
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.delete(new org.apache.hadoop.fs.Path(outputPath), true)
  }
}
