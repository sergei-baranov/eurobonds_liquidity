package com.sergei_baranov.eurobonds_liquidity.consumer

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.{Calendar, Properties, TimeZone}
import java.text.SimpleDateFormat

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import sys.process._
import org.apache.log4j.PropertyConfigurator

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object CalcAnchorDate {
  def calcHour(): Int = {
    val Cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
    Cal.setTimeZone(TimeZone.getTimeZone("Europe/Moscow"))
    val nowHour = Cal.get(Calendar.HOUR_OF_DAY)

    nowHour
  }

  def calcDate(nowHour: Int): String = {
    // если сейчас До 10-ти по Мск - то за todayDate надо считать вчера
    val format = new SimpleDateFormat("yyyMMdd")
    val Cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
    Cal.setTimeZone(TimeZone.getTimeZone("GMT"))
    val gmtTime = Cal.getTime.getTime
    var timezoneAlteredTime = gmtTime + TimeZone.getTimeZone("Europe/Moscow").getRawOffset
    if (nowHour < 10) {
      timezoneAlteredTime -= 86400000;
    }
    Cal.setTimeInMillis(timezoneAlteredTime)
    val dtMillis = Cal.getTime()
    val todayDate = (format.format(dtMillis))

    todayDate
  }
}

object AnalyticsConsumer extends App with LazyLogging {

  // configure log4j
  val log4jProps = new Properties()
  log4jProps.setProperty("log4j.rootLogger", "INFO, stdout")
  log4jProps.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender")
  log4jProps.setProperty("log4j.appender.stdout.target", "System.out")
  log4jProps.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout")
  log4jProps.setProperty("log4j.appender.stdout.layout.ConversionPattern", "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n")
  PropertyConfigurator.configure(log4jProps)

  // prepare mysql tables if not exists
  val driver = "com.mysql.cj.jdbc.Driver"
  val url = "jdbc:mysql://db/test"
  val username = "test"
  val password = "test"
  var connection:Connection = null
  try {
    // make the connection
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)

    // create the statement, and run the select query
    val statement = connection.createStatement()
    statement.executeUpdate("CREATE DATABASE IF NOT EXISTS test;")
    statement.executeUpdate("DROP TABLE IF EXISTS test.bonds_liquidity_metrics;")
    statement.executeUpdate("""
        CREATE TABLE IF NOT EXISTS test.bonds_liquidity_metrics (
          id BIGINT NOT NULL DEFAULT 0,
          isin VARCHAR(64) NOT NULL DEFAULT "",
          date_of_end_placing TIMESTAMP NULL DEFAULT NULL,
          maturity_date TIMESTAMP NULL DEFAULT NULL,
          usd_volume DOUBLE NOT NULL DEFAULT 0,
          bid_ask_spread_relative_median DOUBLE NOT NULL DEFAULT 0,
          bid_ask_spread_relative_rank DOUBLE NOT NULL DEFAULT 0,
          upsert_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id)
        )
        ENGINE = InnoDB;
      """)
    statement.executeUpdate("DROP TABLE IF EXISTS test.quotes_with_liquidity_metrics;")
    /*
    // стримить в мускул у меня не получилось сходу (туду: пробовать с кафка коннектором)
    statement.executeUpdate("""
        CREATE TABLE IF NOT EXISTS test.quotes_with_liquidity_metrics (
          bond BIGINT NOT NULL DEFAULT 0,
          isin VARCHAR(64) NOT NULL DEFAULT "",
          tg BIGINT NOT NULL DEFAULT 0,
          `date` TIMESTAMP NULL DEFAULT NULL,
          volume_money DOUBLE NOT NULL DEFAULT 0,
          bid DOUBLE NOT NULL DEFAULT 0,
          ask DOUBLE NOT NULL DEFAULT 0,
          update_ts TIMESTAMP NULL DEFAULT NULL,
          baspread_rel_median DOUBLE NOT NULL DEFAULT 0,
          baspread_rel_rank DOUBLE NOT NULL DEFAULT 0,
          upsert_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
        ENGINE = InnoDB;
      """)
    //statement.executeUpdate("SET time_zone='+00:00';")
    // эта инструкция должна быть последней внутри try section
    // (ввиду отсутствия IF NOT EXISTS для CREATE INDEX это самый лёгкий путь)
    statement.executeUpdate("CREATE UNIQUE INDEX btd ON test.quotes_with_liquidity_metrics (bond, tg, `date`);")
    */
  } catch {
    case e => e.printStackTrace
  }
  connection.close()

  // make spark jobs

  val appName: String = "My Otus Eurobonds Liquidity"

  val nowHour = CalcAnchorDate.calcHour()
  logger.info("nowHour: [" + nowHour + "]")
  val todayDate = CalcAnchorDate.calcDate(nowHour)
  logger.info("anchorDate: [" + todayDate + "]")

  val (exchangeQuotesFilePath, otcQuotesFilePath, eurobondsFilePath, outputFolder, outputFolderCsv, outputFolderDelta) = (
    "/shara/sergei_baranov_" + todayDate + "/quotes_" + todayDate + ".csv",
    "/shara/sergei_baranov_" + todayDate + "/quotes_mp_month_raw_" + todayDate + ".csv",
    "/shara/sergei_baranov_" + todayDate + "/bonds_" + todayDate + ".csv",
    "/shara/liquidity/parquet_" + todayDate + "/",
    "/shara/liquidity/csv_" + todayDate + "/",
    "/shara/liquidity/delta_" + todayDate + "/"
  )

  "rm -rf " + outputFolder !!

  "rm -rf " + outputFolderCsv !!

  val conf = new SparkConf()
    .setMaster("local")
    .setAppName(appName)

  var needStreamJob = true
  if (nowHour < 10) {
    needStreamJob = false
  }
  Runner.run(conf, exchangeQuotesFilePath, otcQuotesFilePath, eurobondsFilePath, outputFolder, outputFolderCsv, outputFolderDelta, needStreamJob)
}

object Runner extends App with LazyLogging {
  def run(conf: SparkConf,
          exchangeQuotesFilePath: String,
          otcQuotesFilePath: String,
          eurobondsFilePath: String,
          outputFolder: String,
          outputFolderCsv: String,
          outputFolderDelta: String,
          needStreamJob: Boolean = false
         ): Unit = {

    // 1. batch job (calculate bonds liquidity metrics from bonds quotes archive)

    val spark: SparkSession = {
      SparkSession.builder()
        .config(conf)
        .master("local[*]")
        .getOrCreate()
    }

    val exchangeQuotes = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(exchangeQuotesFilePath)

    val otcQuotes = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(otcQuotesFilePath)

    val bondsList = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(eurobondsFilePath)

    val bondsLiquidityMetrics: DataFrame = LiquidityJobber.getLiquidityMetrics(exchangeQuotes, otcQuotes, bondsList)

    bondsLiquidityMetrics.show(40)
    bondsLiquidityMetrics.printSchema()

    // write to parquet
    bondsLiquidityMetrics.coalesce(1)
      .write.format("parquet").save(outputFolder)

    // write to csv
    bondsLiquidityMetrics.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true").save(outputFolderCsv)

    // write to mysql
    val mysqlProps = new java.util.Properties
    mysqlProps.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    mysqlProps.setProperty("user", "test")
    mysqlProps.setProperty("password", "test")
    val url = "jdbc:mysql://db:3306/test"
    val table = "bonds_liquidity_metrics"
    bondsLiquidityMetrics
      // вот такая ерунда у меня на домашней машине (убунта + мариядб),
      // timestamp почему-то в 32 бита, пока что ставлю заплату в коде
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
      .coalesce(1)
      .write
      .mode("append")
      .jdbc(url, table, mysqlProps)

    //
    bondsLiquidityMetrics.unpersist()
    bondsLiquidityMetrics.sparkSession.stop()

    //

    if (false && !needStreamJob) {
      logger.info("Need no stream consumer before 10:00 MSK")
      sys.exit()
    }

    logger.info("====")
    println("====")
    logger.info("outputFolder: " + outputFolder + " (liquidity metrics was saved here in parquet format)")
    println("outputFolder: " + outputFolder + " (liquidity metrics was saved here in parquet format)")
    logger.info("outputFolderCsv: " + outputFolderCsv + " (liquidity metrics was saved here in csv format)")
    println("outputFolderCsv: " + outputFolderCsv + " (liquidity metrics was saved here in csv format)")
    logger.info("outputFolderDelta: " + outputFolderDelta + " (quotes flow will be saving here in delta parquet)")
    println("outputFolderDelta: " + outputFolderDelta + " (quotes flow will be saving here in delta parquet)")
    logger.info("Set this paths to the python notebook cell code variables")
    println("Set this paths to the python notebook cell code variables")
    logger.info("====")
    println("====")

    // 2. stream structured job (enrich bonds quotes stream with liquidity metrics)

    // вот тут он будет работать
    // до бесконечности
    /**
     * @TODO условия выхода в jobber-е
     * @TODO как получать от него выход с некоторой периодичностью?
     * Как гасить его отсюда?
     * разобраться
     */
    QuotesStreamJobber.enrichLiquidityWithIntradayQuotes(outputFolderCsv, outputFolderDelta)
  }
}
