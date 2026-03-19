package com.example.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class BillingPipelineTest extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder()
    .appName("BillingPipelineTest")
    .master("local[*]")
    .getOrCreate()

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("readTransactions should load CSV with correct schema") {
    val path = getClass.getClassLoader.getResource("sample-data/payment_transactions.csv").getPath
    val df = BillingPipeline.readTransactions(spark, path)

    assert(df.columns.toSet === Set(
      "transaction_id", "account_number", "amount",
      "currency", "timestamp", "merchant", "category"
    ))
    assert(df.count() === 30)
  }

  test("aggregateByAccount should group by account_number with correct metrics") {
    import spark.implicits._

    val transactions = Seq(
      ("TXN1", "ACC-001", 100.0, "USD", "2026-03-01T10:00:00Z", "Amazon", "Retail"),
      ("TXN2", "ACC-001", 200.0, "USD", "2026-03-01T11:00:00Z", "Best Buy", "Electronics"),
      ("TXN3", "ACC-001", 50.0, "USD", "2026-03-02T09:00:00Z", "Amazon", "Retail"),
      ("TXN4", "ACC-002", 500.0, "USD", "2026-03-01T10:00:00Z", "Apple", "Electronics"),
      ("TXN5", "ACC-002", 75.0, "USD", "2026-03-02T11:00:00Z", "Starbucks", "Food")
    ).toDF("transaction_id", "account_number", "amount", "currency", "timestamp", "merchant", "category")

    val aggregated = BillingPipeline.aggregateByAccount(transactions)

    assert(aggregated.count() === 2)

    val acc001 = aggregated.filter($"account_number" === "ACC-001").first()
    assert(acc001.getAs[Long]("transaction_count") === 3)
    assert(acc001.getAs[Double]("total_volume") === 350.0)
    assert(acc001.getAs[Long]("unique_merchants") === 2) // Amazon, Best Buy
    assert(acc001.getAs[Long]("unique_categories") === 2) // Retail, Electronics

    val acc002 = aggregated.filter($"account_number" === "ACC-002").first()
    assert(acc002.getAs[Long]("transaction_count") === 2)
    assert(acc002.getAs[Double]("total_volume") === 575.0)
  }

  test("computeTieredFee should calculate correct fee for Tier 1") {
    // 500 transactions at $0.30 each
    val fee = BillingPipeline.computeTieredFee(500)
    assert(fee === 150.0)
  }

  test("computeTieredFee should calculate correct fee spanning Tier 1 and Tier 2") {
    // 1,500 transactions: 1,000 * $0.30 + 500 * $0.25 = $300 + $125 = $425
    val fee = BillingPipeline.computeTieredFee(1500)
    assert(fee === 425.0)
  }

  test("computeTieredFee should calculate correct fee spanning all tiers") {
    // 150,000 transactions:
    // 1,000 * $0.30 = $300
    // 9,000 * $0.25 = $2,250
    // 90,000 * $0.15 = $13,500
    // 50,000 * $0.08 = $4,000
    // Total = $20,050
    val fee = BillingPipeline.computeTieredFee(150000)
    assert(fee === 20050.0)
  }

  test("applyPricing should produce billing report with all required columns") {
    import spark.implicits._

    val transactions = Seq(
      ("TXN1", "ACC-001", 100.0, "USD", "2026-03-01T10:00:00Z", "Amazon", "Retail"),
      ("TXN2", "ACC-001", 200.0, "USD", "2026-03-01T11:00:00Z", "Best Buy", "Electronics"),
      ("TXN3", "ACC-002", 500.0, "USD", "2026-03-01T10:00:00Z", "Apple", "Electronics")
    ).toDF("transaction_id", "account_number", "amount", "currency", "timestamp", "merchant", "category")

    val aggregated = BillingPipeline.aggregateByAccount(transactions)
    val report = BillingPipeline.applyPricing(aggregated)

    val expectedColumns = Set(
      "account_number", "transaction_count", "total_volume",
      "avg_transaction_amount", "min_transaction_amount", "max_transaction_amount",
      "unique_merchants", "unique_categories",
      "tiered_fee", "processing_fee", "platform_fee",
      "total_billing_amount", "effective_rate_per_txn"
    )
    assert(report.columns.toSet === expectedColumns)

    // ACC-001: 2 txns, $300 volume
    // tiered_fee = 2 * 0.30 = $0.60
    // processing_fee = 300 * 0.029 + 2 * 0.10 = $8.70 + $0.20 = $8.90
    // platform_fee = $49.99
    // total = $0.60 + $8.90 + $49.99 = $59.49
    val acc001 = report.filter($"account_number" === "ACC-001").first()
    assert(acc001.getAs[Double]("tiered_fee") === 0.60)
    assert(acc001.getAs[Double]("processing_fee") === 8.9)
    assert(acc001.getAs[Double]("platform_fee") === 49.99)
    assert(acc001.getAs[Double]("total_billing_amount") === 59.49)
  }

  test("generateSummary should produce correct aggregate totals") {
    import spark.implicits._

    val transactions = Seq(
      ("TXN1", "ACC-001", 100.0, "USD", "2026-03-01T10:00:00Z", "Amazon", "Retail"),
      ("TXN2", "ACC-001", 200.0, "USD", "2026-03-01T11:00:00Z", "Best Buy", "Electronics"),
      ("TXN3", "ACC-002", 500.0, "USD", "2026-03-01T10:00:00Z", "Apple", "Electronics")
    ).toDF("transaction_id", "account_number", "amount", "currency", "timestamp", "merchant", "category")

    val aggregated = BillingPipeline.aggregateByAccount(transactions)
    val report = BillingPipeline.applyPricing(aggregated)
    val summary = BillingPipeline.generateSummary(report)

    val row = summary.first()
    assert(row.getAs[Long]("total_accounts") === 2)
    assert(row.getAs[Long]("total_transactions") === 3)
    assert(row.getAs[Double]("total_payment_volume") === 800.0)
  }

  test("enrichForStorage should add generated_at and billing_period columns") {
    import spark.implicits._

    val transactions = Seq(
      ("TXN1", "ACC-001", 100.0, "USD", "2026-03-01T10:00:00Z", "Amazon", "Retail"),
      ("TXN2", "ACC-002", 500.0, "USD", "2026-03-01T10:00:00Z", "Apple", "Electronics")
    ).toDF("transaction_id", "account_number", "amount", "currency", "timestamp", "merchant", "category")

    val aggregated = BillingPipeline.aggregateByAccount(transactions)
    val report = BillingPipeline.applyPricing(aggregated)
    val enriched = BillingPipeline.enrichForStorage(report)

    assert(enriched.columns.contains("generated_at"))
    assert(enriched.columns.contains("billing_period"))
    assert(enriched.count() === 2)

    val billingPeriod = enriched.select("billing_period").first().getString(0)
    assert(billingPeriod.matches("\\d{4}-\\d{2}"))
  }

  test("saveToMonitoringBills should persist data to monitoring_bills table") {
    import spark.implicits._

    val transactions = Seq(
      ("TXN1", "ACC-001", 100.0, "USD", "2026-03-01T10:00:00Z", "Amazon", "Retail"),
      ("TXN2", "ACC-001", 200.0, "USD", "2026-03-01T11:00:00Z", "Best Buy", "Electronics"),
      ("TXN3", "ACC-002", 500.0, "USD", "2026-03-01T10:00:00Z", "Apple", "Electronics")
    ).toDF("transaction_id", "account_number", "amount", "currency", "timestamp", "merchant", "category")

    val aggregated = BillingPipeline.aggregateByAccount(transactions)
    val report = BillingPipeline.applyPricing(aggregated)

    // Drop table if it exists from a previous test run
    spark.sql("DROP TABLE IF EXISTS monitoring_bills")

    BillingPipeline.saveToMonitoringBills(spark, report)

    val savedTable = spark.table("monitoring_bills")
    assert(savedTable.count() === 2)

    // Verify all expected columns exist
    val expectedColumns = Set(
      "account_number", "transaction_count", "total_volume",
      "avg_transaction_amount", "min_transaction_amount", "max_transaction_amount",
      "unique_merchants", "unique_categories",
      "tiered_fee", "processing_fee", "platform_fee",
      "total_billing_amount", "effective_rate_per_txn",
      "generated_at", "billing_period"
    )
    assert(expectedColumns.subsetOf(savedTable.columns.toSet))

    // Verify append mode: save again and check row count doubles
    BillingPipeline.saveToMonitoringBills(spark, report)
    val updatedTable = spark.table("monitoring_bills")
    assert(updatedTable.count() === 4)

    // Clean up
    spark.sql("DROP TABLE IF EXISTS monitoring_bills")
  }
}
