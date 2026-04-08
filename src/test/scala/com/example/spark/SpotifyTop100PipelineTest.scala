package com.example.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class SpotifyTop100PipelineTest extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder()
    .appName("SpotifyTop100PipelineTest")
    .master("local[*]")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("readSongs should load CSV with correct schema and row count") {
    val path = getClass.getClassLoader.getResource("sample-data/spotify_top100_sample.csv").getPath
    val df = SpotifyTop100Pipeline.readSongs(spark, path)

    assert(df.columns.contains("name"))
    assert(df.columns.contains("artists"))
    assert(df.columns.contains("danceability"))
    assert(df.count() === 8)
  }

  test("artistsWithMostSongs should rank artists by song count") {
    import spark.implicits._

    val path = this.getClass.getClassLoader.getResource("sample-data/spotify_top100_sample.csv").getPath
    val songs = SpotifyTop100Pipeline.readSongs(spark, path)
    val result = SpotifyTop100Pipeline.artistsWithMostSongs(songs)

    assert(result.columns.toSet === Set("artists", "song_count", "songs"))

    // Drake and Post Malone each have 2 songs in the sample
    val topArtist = result.first()
    assert(topArtist.getAs[Long]("song_count") >= 2)

    val drake = result.filter($"artists" === "Drake").first()
    assert(drake.getAs[Long]("song_count") === 2)

    val postMalone = result.filter($"artists" === "Post Malone").first()
    assert(postMalone.getAs[Long]("song_count") === 2)
  }

  test("lilVsDjComparison should correctly count Lil vs DJ artists") {
    import spark.implicits._

    val path = this.getClass.getClassLoader.getResource("sample-data/spotify_top100_sample.csv").getPath
    val songs = SpotifyTop100Pipeline.readSongs(spark, path)
    val result = SpotifyTop100Pipeline.lilVsDjComparison(songs)

    assert(result.columns.toSet === Set("artist_category", "artist_count", "song_count", "artists"))

    val lil = result.filter($"artist_category" === "Lil").first()
    assert(lil.getAs[Long]("artist_count") === 1)
    assert(lil.getAs[Long]("song_count") === 1)

    val dj = result.filter($"artist_category" === "DJ").first()
    assert(dj.getAs[Long]("artist_count") === 1)
    assert(dj.getAs[Long]("song_count") === 1)
  }

  test("attributeCorrelations should return pairwise correlations with correct schema") {
    val path = getClass.getClassLoader.getResource("sample-data/spotify_top100_sample.csv").getPath
    val songs = SpotifyTop100Pipeline.readSongs(spark, path)
    val result = SpotifyTop100Pipeline.attributeCorrelations(songs)

    assert(result.columns.toSet === Set("attribute_1", "attribute_2", "correlation", "abs_correlation"))

    // With 10 numeric attributes, we should have C(10,2) = 45 pairs
    assert(result.count() === 45)

    // All abs_correlation values should be between 0 and 1
    import spark.implicits._
    val outOfRange = result.filter($"abs_correlation" < 0 || $"abs_correlation" > 1.0001)
    assert(outOfRange.count() === 0)
  }

  test("attributeVariability should return stats for all numeric attributes") {
    val path = getClass.getClassLoader.getResource("sample-data/spotify_top100_sample.csv").getPath
    val songs = SpotifyTop100Pipeline.readSongs(spark, path)
    val result = SpotifyTop100Pipeline.attributeVariability(songs)

    assert(result.columns.toSet === Set("attribute", "mean", "stddev", "coeff_of_variation"))
    assert(result.count() === SpotifyTop100Pipeline.NUMERIC_ATTRS.length)

    // All stddev values should be non-negative
    import spark.implicits._
    val negativeStddev = result.filter($"stddev" < 0)
    assert(negativeStddev.count() === 0)
  }
}
