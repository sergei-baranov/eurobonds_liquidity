package com.sergei_baranov.eurobonds_liquidity.consumer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, Trigger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.ForeachWriter

object QuotesStreamJobber extends App with LazyLogging {
  /**
   *
   * @param bondsLiquidityMetricsDirPathCsv
   */
  def enrichLiquidityWithIntradayQuotes(bondsLiquidityMetricsDirPathCsv: String, outputFolderDelta: String): Unit = {

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

    val bondsLiquidityMetrics = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(bondsLiquidityMetricsDirPathCsv)
      // вот такая ерунда у меня на домашней машине (убунта + мариядб),
      // timestamp почему-то в 32 бита, пока что ставлю заплату в коде
      /*
      // закомментировано, так как стрим в МуСкул мне настроить не удалось
      .withColumn(
        "maturity_date",
        when(
          (
            col("maturity_date") > "2037-12-30 00:00:00"
            ),
          "1970-01-01 04:00:01")
          .otherwise(col("maturity_date"))
      )
      .withColumn(
        "date_of_end_placing",
        when(
          (
            col("date_of_end_placing") > "2037-12-30 00:00:00"
            ),
          "1970-01-01 04:00:01")
          .otherwise(col("date_of_end_placing"))
      )
      */
    // broadcast - с метриками будем джойнить поток
    val bondsLiquidityMetricsBroadcasted = broadcast(bondsLiquidityMetrics)

    // https://databricks.com/blog/2017/02/23/working-complex-data-formats-structured-streaming-apache-spark-2-1.html
    // https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time
    // http://vishnuviswanath.com/spark_structured_streaming.html
    // https://stackoverflow.com/questions/45205969/update-ttl-for-a-particular-topic-in-kafka-using-java
    // https://databricks.com/blog/2017/05/08/event-time-aggregation-watermarking-apache-sparks-structured-streaming.html
    // https://databricks.com/blog/2018/03/13/introducing-stream-stream-joins-in-apache-spark-2-3.html

    val expectedSchema = new StructType()
      .add(StructField("id", StringType)) // LongType
      .add(StructField("bond", StringType)) // LongType
      .add(StructField("bid", StringType)) // DoubleType
      .add(StructField("ask", StringType)) // DoubleType
      .add(StructField("tg", StringType)) // LongType
      .add(StructField("date", StringType)) // TimestampType
      .add(StructField("update_ts", StringType)) // TimestampType
      .add(StructField("volume", StringType)) // DoubleType
      .add(StructField("volume_money", StringType)) // DoubleType
      .add(StructField("overturn", StringType)) // DoubleType

    /*
    Тут пока достаточно искусственная задача - обогащать метрики ликвидности значениями
    текущего объёма торгов
    Искусственная - потому что на входе поток не совсем корректный, для обучения взяли,
    что есть, далее на источние кое-что поменяем по ходу доработки проекта и методологии
    */
    //val winByTsDesc = Window.partitionBy($"bond", $"tg").orderBy($"update_ts".desc_nulls_last)
    val watermarkMinutes = 120;
    val winDurationMinutes = 30;
    val slideDurationMinutes = 1;
    val triggerProcTimeMinutes = 1; // пускай выдаёт пустые, для наглядности, что не исдох
    val transformedStream: DataFrame = inDf
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS STRING)")
        .as[(String, String, String)]
      .select($"value", $"timestamp")
      .select(from_json($"value", expectedSchema).as("data"))
      .select("data.*")
      .select(
        $"bond".cast(LongType).as("bond"),
        $"tg".cast(LongType).as("tg"),
        $"volume_money".cast(DoubleType).as("volume_money"),
        $"bid".cast(DoubleType).as("bid"),
        $"ask".cast(DoubleType).as("ask"),
        $"overturn".cast(DoubleType).as("overturn"),
        //$"date".as("date_str"),
        $"date".cast(DateType).as("date"),
        //$"update_ts".as("update_ts_str"),
        $"update_ts".cast(TimestampType).as("update_ts")
      )
      .filter(
        $"bond".isNotNull
        && $"tg".isNotNull
      )
      .withWatermark("update_ts", watermarkMinutes + " minutes")
      .groupBy(
        window(
          $"update_ts",
          winDurationMinutes + " minutes",
          slideDurationMinutes + " minutes"
        )
        ,$"bond", $"tg", $"date"
      )
      .agg(
        first($"bond").as("q_bond"),
        first($"tg").as("q_tg"),
        first($"date").as("q_date"),
        sum($"volume_money").as("date_sum_volume_money"),
        max($"bid").as("date_max_bid"),
        max($"ask").as("date_max_ask"),
        max($"update_ts").as("last_update_ts"),
        count($"update_ts").as("cnt")
      )
      .select(
        $"q_bond", $"q_tg", $"q_date",
        $"date_sum_volume_money", $"date_max_bid", $"date_max_ask",
        $"last_update_ts", $"cnt"
      )
      //.filter($"cnt" === 1) // чтобы не агрегировать лишнее
      .select("*")
      .as("quot")
      .join(
        bondsLiquidityMetricsBroadcasted.as("liq"),
        $"liq.id" === $"quot.q_bond",
        "left"
      )
      .select(
        $"q_bond".as("bond"),
        $"isin",
        $"q_tg".as("tg"),
        $"q_date".as("date"),
        $"date_sum_volume_money".as("volume_money"),
        $"date_max_bid".as("bid"),
        $"date_max_ask".as("ask"),
        $"last_update_ts".as("update_ts"),
        $"bid_ask_spread_relative_median".as("baspread_rel_median"),
        $"bid_ask_spread_relative_rank".as("baspread_rel_rank"),
        $"cnt"
      )
      //.orderBy($"last_update_ts".desc)

    transformedStream.writeStream
      .outputMode("append")
      .format("delta")
      //.trigger(Trigger.ProcessingTime(triggerProcTimeMinutes + " minutes"))
      .option("checkpointLocation", "/storage/analytics-consumer/checkpoints")
      .start(outputFolderDelta)

    /*
    val url = "jdbc:mysql://db:3306/test"
    val user ="test"
    val pwd = "test"

    val writer = new JDBCSink(url, user, pwd)
    val query =
      transformedStream
        .writeStream
        .foreach(writer)
        .outputMode("update")
        //.trigger(triggerProcTimeMinutes + " minutes")
        .start()
    */

    /*
    transformedStream
      .writeStream
      .outputMode("append")
      .format("console")
      //.trigger(Trigger.ProcessingTime(triggerProcTimeMinutes + " minutes"))
      .start()
    */

    spark.streams.awaitAnyTermination()
  }

}
