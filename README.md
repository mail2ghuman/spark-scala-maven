# Spark Scala Maven

A basic Apache Spark application built with Scala and Maven.

## Prerequisites

- Java 8 or 11
- Maven 3.6+
- Apache Spark 3.5.x (for `spark-submit`)

## Project Structure

```
spark-scala-maven/
├── pom.xml
├── src/
│   ├── main/
│   │   ├── scala/com/example/spark/
│   │   │   └── SparkApp.scala
│   │   └── resources/
│   │       └── log4j2.properties
│   └── test/
│       └── scala/com/example/spark/
│           └── SparkAppTest.scala
└── README.md
```

## Build

```bash
mvn clean package
```

## Run Locally

```bash
# Using Maven (for development)
mvn exec:java -Dexec.mainClass="com.example.spark.SparkApp"

# Using spark-submit (production-like)
spark-submit \
  --class com.example.spark.SparkApp \
  --master local[*] \
  target/spark-scala-maven-1.0.0-SNAPSHOT.jar
```

## Run Tests

```bash
mvn test
```

## Features

- **DataFrame API**: Creates and queries a sample DataFrame
- **RDD Word Count**: Classic MapReduce word count using Spark RDDs
- **ScalaTest**: Unit tests with SparkSession lifecycle management
- **Fat JAR**: Maven Shade plugin packages a deployable uber-JAR
