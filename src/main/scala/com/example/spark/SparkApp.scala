package com.example.spark

import org.apache.spark.sql.SparkSession

object SparkApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Scala Maven App")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    println("=== Spark Scala Maven App ===")
    println(s"Spark version: ${spark.version}")

    // Sample data
    val data = Seq(
      ("Alice", 30),
      ("Bob", 25),
      ("Charlie", 35),
      ("Diana", 28),
      ("Eve", 32)
    )

    val df = data.toDF("name", "age")

    println("\n--- Sample DataFrame ---")
    df.show()

    println("--- Basic Statistics ---")
    df.describe("age").show()

    println(s"--- People older than 29 ---")
    df.filter($"age" > 29).show()

    // Word count example using RDD
    val words = spark.sparkContext.parallelize(
      Seq("hello world", "hello spark", "spark is great", "hello great world")
    )
    val wordCounts = words
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect()

    println("--- Word Count ---")
    wordCounts.sortBy(-_._2).foreach { case (word, count) =>
      println(s"  $word: $count")
    }

    spark.stop()
    println("\nSpark application completed successfully.")
  }
}
