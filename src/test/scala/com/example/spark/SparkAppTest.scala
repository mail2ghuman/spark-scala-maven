package com.example.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class SparkAppTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("SparkAppTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("DataFrame should be created with correct schema") {
    import spark.implicits._

    val data = Seq(("Alice", 30), ("Bob", 25))
    val df = data.toDF("name", "age")

    assert(df.columns.toSet === Set("name", "age"))
    assert(df.count() === 2)
  }

  test("DataFrame filter should return correct results") {
    import spark.implicits._

    val data = Seq(("Alice", 30), ("Bob", 25), ("Charlie", 35))
    val df = data.toDF("name", "age")

    val filtered = df.filter($"age" > 29)
    assert(filtered.count() === 2)
  }

  test("Word count should produce correct results") {
    val words = spark.sparkContext.parallelize(
      Seq("hello world", "hello spark")
    )
    val wordCounts = words
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collect()
      .toMap

    assert(wordCounts("hello") === 2)
    assert(wordCounts("world") === 1)
    assert(wordCounts("spark") === 1)
  }
}
