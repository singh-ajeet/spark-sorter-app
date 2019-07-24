package com.ajeet.bigdata.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * spark2-submit  --class com.ajeet.bigdata.spark.SorterApp
  * C:\Users\aj001si\Downloads\testData_1M.txt G:\TestData\1million '|' 7 --master yarn --deploy-mode cluster
  * spark-sorter-app-1.0-SNAPSHOT.jar
  */
object SorterApp  {

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)
    val delimiter = args(2)
    val columnIndex = Integer.parseInt(args(3))

    val spark = SparkSession.builder()
      .appName("Spark records sorting app")
      .master("local")
      .getOrCreate()

    val df = spark.read
      .format("csv")
      .option("delimiter", delimiter)
      .option("inferScheme", "true")
      .load(input)

    val columnToSort = df.columns(columnIndex)
    val sortedDf = df.orderBy(columnToSort)

    sortedDf.show()

    sortedDf
      .coalesce(1)
      .write
      .format("csv")
      .option("delimiter", delimiter)
      .mode(SaveMode.Overwrite)
      //   .option("delimiter", delimiter)
      .save(output)
  }
}
