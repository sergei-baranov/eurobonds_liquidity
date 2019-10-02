package com.sergei_baranov.eurobonds_liquidity.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, Trigger}

object QuotesStreamJobber extends App with LazyLogging {
  /**
   *
   * @param bondsLiquidityMetrics
   */
  def enrichLiquidityWithIntradayQuotes(bondsLiquidityMetrics: DataFrame): Unit = {

    logger.info("getOrCreate SparkSession")
    println("getOrCreate SparkSession")

    val appName: String = "quotes-stream-jobber"

    val ssBuilder: SparkSession.Builder = SparkSession.builder()
    ssBuilder.appName(appName)
    ssBuilder.master("local[2]")
    ssBuilder.config("spark.driver.memory", "5g")

    val spark: SparkSession = ssBuilder.getOrCreate()

    /*
    logger.info("getActiveSession SparkSession")
    println("getActiveSession SparkSession")
    val spark: SparkSession = SparkSession.getActiveSession.get
    */

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN") //("ALL")

    logger.info("Initializing Structured consumer")
    println("Initializing Structured consumer")

    val inDataStreamReader: DataStreamReader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "eurobonds-quotes-topic")
      .option("startingOffsets", "earliest")

    logger.info("loading stream")
    println("loading stream")

    val inDf: DataFrame = inDataStreamReader
      .load()

    logger.info("loaded")
    println("loaded")

    val expectedSchema = new StructType()
      .add(StructField("id", LongType))
      .add(StructField("bond", LongType))
      .add(StructField("bid", DoubleType))
      .add(StructField("ask", DoubleType))
      .add(StructField("tg", LongType))
      .add(StructField("date", TimestampType))
      .add(StructField("update_ts", TimestampType))
      .add(StructField("volume", DoubleType))
      .add(StructField("volume_money", DoubleType))
      .add(StructField("overturn", DoubleType))

    val transformedStream: DataFrame = inDf
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]
      .select($"value")
      //.select(from_json($"value", expectedSchema).as("data"))
      //.select("data.*")
      //.withWatermark("update_ts", "60 minutes")
    /**
     * @TODO from_json заполняет всё null-ами. Продолжить с этого места
     */

    transformedStream
      .writeStream
      .outputMode("append")
      .format("console") // "delta"
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()

    /**
     * @TODO отправлять это всё надо в МуСкул на апсерт, то есть в кафку в jdbc silk connector
     * Вопрос: как по дороге сохранять ещё и на локалке, например в дельту?
     */

    /*
    transformedStream.writeStream
      .outputMode("append")
      .format("delta")
      .trigger(Trigger.ProcessingTime("5 minutes"))
      .option("checkpointLocation", "/storage/analytics-consumer/checkpoints")
      .start("/storage/analytics-consumer/output/csv")
    */

    spark.streams.awaitAnyTermination()
  }

}
