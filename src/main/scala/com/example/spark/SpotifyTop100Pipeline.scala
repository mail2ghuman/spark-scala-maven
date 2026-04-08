package com.example.spark

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}

/**
 * SpotifyTop100Pipeline — Analytics report on the Spotify Top 100 songs of 2018.
 *
 * JIRA: KAN-4
 *
 * Reads top2018.csv.zip and produces the following analytics (printed to stdout):
 *   1. Artists with the most Top 100 songs
 *   1b. Artists with the most Top 10 songs
 *   2. Comparison: artists with "Lil" vs "DJ" in their name
 *   3. Most strongly correlated song attributes
 *   4. Attribute variability (stddev) — most and least variable
 */
object SpotifyTop100Pipeline {

  /** Numeric attribute columns used for correlation and variability analysis. */
  val NUMERIC_ATTRS: Seq[String] = Seq(
    "danceability", "energy", "loudness", "speechiness",
    "acousticness", "instrumentalness", "liveness", "valence", "tempo", "duration_ms"
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spotify Top 100 Analytics")
      .master("local[*]")
      .getOrCreate()

    val inputPath = if (args.length > 0) args(0) else "top2018.csv.zip"

    println("=== Spotify Top 100 Analytics Report (2018) ===")
    println(s"Input path: $inputPath")

    val songs = readSongs(spark, inputPath)

    println(s"\nTotal songs loaded: ${songs.count()}")

    // 1. Artists with the most Top 100 songs
    println("\n" + "=" * 60)
    println("1. Artists with the Most Top 100 Songs")
    println("=" * 60)
    val topArtists = artistsWithMostSongs(songs)
    topArtists.show(20, truncate = false)

    // 1b. Artists with the most Top 10 songs
    println("\n" + "=" * 60)
    println("1b. Artists with the Most Top 10 Songs")
    println("=" * 60)
    val top10Artists = artistsWithMostTop10Songs(songs)
    top10Artists.show(10, truncate = false)

    // 2. "Lil" vs "DJ" artists
    println("\n" + "=" * 60)
    println("2. Artists with 'Lil' vs 'DJ' in Their Name")
    println("=" * 60)
    val lilVsDj = lilVsDjComparison(songs)
    lilVsDj.show(truncate = false)

    // 3. Most strongly correlated attributes
    println("\n" + "=" * 60)
    println("3. Song Attribute Correlations")
    println("=" * 60)
    val correlations = attributeCorrelations(songs)
    println("\nTop 10 most strongly correlated attribute pairs:")
    correlations.show(10, truncate = false)

    println("\nTop 10 least correlated (most independent) attribute pairs:")
    val leastCorrelated = correlations.orderBy(col("abs_correlation").asc)
    leastCorrelated.show(10, truncate = false)

    // 4. Attribute variability
    println("\n" + "=" * 60)
    println("4. Attribute Variability (Standard Deviation & Coefficient of Variation)")
    println("=" * 60)
    val variability = attributeVariability(songs)
    println("\nAttributes sorted by coefficient of variation (most variable first):")
    variability.show(truncate = false)

    spark.stop()
    println("\nSpotify Top 100 Analytics completed successfully.")
  }

  /** Read the Spotify Top 100 CSV (supports both .csv and .csv.zip). */
  def readSongs(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  /**
   * 1. Identify artists that had the most Top 100 songs.
   * Returns a DataFrame of (artists, song_count, songs) ordered by song_count desc.
   */
  def artistsWithMostSongs(songs: DataFrame): DataFrame = {
    songs.groupBy("artists")
      .agg(
        count("name").as("song_count"),
        concat_ws(", ", collect_list("name")).as("songs")
      )
      .orderBy(desc("song_count"))
  }

  /**
   * 1b. Identify artists that had the most Top 10 songs.
   * The CSV is ordered by ranking, so we assign a rank via row_number
   * and filter to the top 10.
   * Returns a DataFrame of (artists, song_count, songs) ordered by song_count desc.
   */
  def artistsWithMostTop10Songs(songs: DataFrame): DataFrame = {
    val spark = songs.sparkSession
    val schema = songs.schema
    val indexedRdd = songs.rdd.zipWithIndex().map { case (row, idx) =>
      Row.fromSeq(row.toSeq :+ (idx + 1L))
    }
    val newSchema = StructType(schema.fields :+ StructField("rank", LongType, nullable = false))
    val withRank = spark.createDataFrame(indexedRdd, newSchema)
    val top10 = withRank.filter(col("rank") <= 10)

    top10.groupBy("artists")
      .agg(
        count("name").as("song_count"),
        concat_ws(", ", collect_list("name")).as("songs")
      )
      .orderBy(desc("song_count"))
  }

  /**
   * 2. Compare artists with "Lil" vs "DJ" in their name.
   * Returns a DataFrame with category, artist_count, and the artist names.
   */
  def lilVsDjComparison(songs: DataFrame): DataFrame = {
    val withCategory = songs
      .withColumn("artist_category",
        when(upper(col("artists")).contains("LIL"), "Lil")
          .when(upper(col("artists")).contains("DJ"), "DJ")
          .otherwise("Other")
      )

    val lilDjOnly = withCategory.filter(col("artist_category") =!= "Other")

    lilDjOnly.groupBy("artist_category")
      .agg(
        countDistinct("artists").as("artist_count"),
        count("name").as("song_count"),
        concat_ws(", ", collect_set("artists")).as("artists")
      )
      .orderBy(desc("song_count"))
  }

  /**
   * 3. Compute pairwise Pearson correlations between numeric attributes.
   * Returns a DataFrame of (attribute_1, attribute_2, correlation, abs_correlation)
   * sorted by abs_correlation desc.
   */
  def attributeCorrelations(songs: DataFrame): DataFrame = {
    val spark = songs.sparkSession
    import spark.implicits._

    val pairs = for {
      i <- NUMERIC_ATTRS.indices
      j <- (i + 1) until NUMERIC_ATTRS.length
    } yield (NUMERIC_ATTRS(i), NUMERIC_ATTRS(j))

    val correlationRows = pairs.map { case (attr1, attr2) =>
      val corr = songs.stat.corr(attr1, attr2)
      val corrRounded = if (corr.isNaN) Double.NaN
        else BigDecimal(corr).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
      (attr1, attr2, corrRounded, if (corr.isNaN) Double.NaN else math.abs(corr))
    }

    correlationRows.toDF("attribute_1", "attribute_2", "correlation", "abs_correlation")
      .orderBy(desc("abs_correlation"))
  }

  /**
   * 4. Compute variability metrics for each numeric attribute.
   * Returns a DataFrame of (attribute, mean, stddev, coeff_of_variation)
   * sorted by coeff_of_variation desc.
   *
   * Coefficient of variation = stddev / |mean| — allows comparison across
   * attributes with different scales.
   */
  def attributeVariability(songs: DataFrame): DataFrame = {
    val spark = songs.sparkSession
    import spark.implicits._

    val stats = NUMERIC_ATTRS.map { attr =>
      val row = songs.agg(
        avg(col(attr)).as("mean_val"),
        stddev(col(attr)).as("stddev_val")
      ).first()

      val meanVal = row.getDouble(0)
      val stddevVal = row.getDouble(1)
      val cv = if (meanVal.isNaN || stddevVal.isNaN || math.abs(meanVal) < 1e-10) 0.0
        else stddevVal / math.abs(meanVal)

      def safeRound(v: Double): Double =
        if (v.isNaN) Double.NaN
        else BigDecimal(v).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

      (attr, safeRound(meanVal), safeRound(stddevVal), safeRound(cv))
    }

    stats.toDF("attribute", "mean", "stddev", "coeff_of_variation")
      .orderBy(desc("coeff_of_variation"))
  }
}
