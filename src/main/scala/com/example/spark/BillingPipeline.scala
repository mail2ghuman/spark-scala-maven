package com.example.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * BillingPipeline reads payment transaction data, aggregates by account number,
 * and generates a billing report using a standard tiered pricing model.
 *
 * Expected input schema (CSV):
 *   transaction_id: String
 *   account_number: String
 *   amount: Double
 *   currency: String
 *   timestamp: String (ISO 8601)
 *   merchant: String
 *   category: String
 *
 * Standard Tiered Pricing Model:
 *   Tier 1: 0 - 1,000 transactions      -> $0.30 per transaction
 *   Tier 2: 1,001 - 10,000 transactions  -> $0.25 per transaction
 *   Tier 3: 10,001 - 100,000 transactions -> $0.15 per transaction
 *   Tier 4: 100,001+ transactions         -> $0.08 per transaction
 *
 *   Processing fee: 2.9% of total transaction volume + $0.10 per transaction
 *   Monthly platform fee: $49.99 per active account
 */
object BillingPipeline {

  val TIER_1_LIMIT = 1000
  val TIER_2_LIMIT = 10000
  val TIER_3_LIMIT = 100000

  val TIER_1_RATE = 0.30
  val TIER_2_RATE = 0.25
  val TIER_3_RATE = 0.15
  val TIER_4_RATE = 0.08

  val PROCESSING_FEE_PERCENT = 0.029
  val PER_TRANSACTION_FEE = 0.10
  val MONTHLY_PLATFORM_FEE = 49.99

  val transactionSchema: StructType = StructType(Seq(
    StructField("transaction_id", StringType, nullable = false),
    StructField("account_number", StringType, nullable = false),
    StructField("amount", DoubleType, nullable = false),
    StructField("currency", StringType, nullable = false),
    StructField("timestamp", StringType, nullable = false),
    StructField("merchant", StringType, nullable = false),
    StructField("category", StringType, nullable = false)
  ))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Payment Billing Pipeline")
      .master("local[*]")
      .getOrCreate()

    val inputPath = if (args.length > 0) args(0) else "hdfs:///data/payment_transactions"
    val outputPath = if (args.length > 1) args(1) else "hdfs:///data/billing_reports"

    println("=== Payment Billing Pipeline ===")
    println(s"Input path:  $inputPath")
    println(s"Output path: $outputPath")

    val transactions = readTransactions(spark, inputPath)
    val aggregated = aggregateByAccount(transactions)
    val billingReport = applyPricing(aggregated)

    println("\n--- Billing Report ---")
    billingReport.show(50, truncate = false)

    billingReport.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputPath)

    println(s"\nBilling report written to: $outputPath")

    val summary = generateSummary(billingReport)
    summary.show(truncate = false)

    spark.stop()
    println("Billing pipeline completed successfully.")
  }

  /** Read payment transaction data from the given path. */
  def readTransactions(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .schema(transactionSchema)
      .csv(path)
  }

  /** Aggregate transactions by account number. */
  def aggregateByAccount(transactions: DataFrame): DataFrame = {
    transactions.groupBy("account_number")
      .agg(
        count("transaction_id").as("transaction_count"),
        sum("amount").as("total_volume"),
        avg("amount").as("avg_transaction_amount"),
        min("amount").as("min_transaction_amount"),
        max("amount").as("max_transaction_amount"),
        countDistinct("merchant").as("unique_merchants"),
        countDistinct("category").as("unique_categories")
      )
  }

  /** Apply tiered pricing model to aggregated account data. */
  def applyPricing(aggregated: DataFrame): DataFrame = {
    val calculateTieredFee = udf((txnCount: Long) => computeTieredFee(txnCount))

    aggregated
      .withColumn("tiered_fee", calculateTieredFee(col("transaction_count")))
      .withColumn("processing_fee",
        round(col("total_volume") * PROCESSING_FEE_PERCENT + col("transaction_count") * PER_TRANSACTION_FEE, 2))
      .withColumn("platform_fee", lit(MONTHLY_PLATFORM_FEE))
      .withColumn("total_billing_amount",
        round(col("tiered_fee") + col("processing_fee") + col("platform_fee"), 2))
      .withColumn("effective_rate_per_txn",
        round(col("total_billing_amount") / col("transaction_count"), 4))
      .withColumn("total_volume", round(col("total_volume"), 2))
      .withColumn("avg_transaction_amount", round(col("avg_transaction_amount"), 2))
      .orderBy(desc("total_billing_amount"))
  }

  /**
   * Compute the tiered transaction fee.
   * Each tier applies only to the transactions within that tier's range.
   */
  def computeTieredFee(txnCount: Long): Double = {
    var remaining = txnCount
    var fee = 0.0

    // Tier 1: first 1,000
    val tier1 = math.min(remaining, TIER_1_LIMIT)
    fee += tier1 * TIER_1_RATE
    remaining -= tier1

    // Tier 2: next 9,000 (1,001 - 10,000)
    if (remaining > 0) {
      val tier2 = math.min(remaining, TIER_2_LIMIT - TIER_1_LIMIT)
      fee += tier2 * TIER_2_RATE
      remaining -= tier2
    }

    // Tier 3: next 90,000 (10,001 - 100,000)
    if (remaining > 0) {
      val tier3 = math.min(remaining, TIER_3_LIMIT - TIER_2_LIMIT)
      fee += tier3 * TIER_3_RATE
      remaining -= tier3
    }

    // Tier 4: everything above 100,000
    if (remaining > 0) {
      fee += remaining * TIER_4_RATE
    }

    BigDecimal(fee).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  /** Generate a high-level summary of the billing report. */
  def generateSummary(billingReport: DataFrame): DataFrame = {
    billingReport.agg(
      count("account_number").as("total_accounts"),
      sum("transaction_count").as("total_transactions"),
      round(sum("total_volume"), 2).as("total_payment_volume"),
      round(sum("total_billing_amount"), 2).as("total_billing_revenue"),
      round(avg("total_billing_amount"), 2).as("avg_billing_per_account"),
      round(avg("effective_rate_per_txn"), 4).as("avg_effective_rate")
    )
  }
}
