package com.sergei_baranov.eurobonds_liquidity.consumer

import java.util.{Calendar, TimeZone}
import java.text.SimpleDateFormat

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}

import sys.process._

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

  val appName: String = "My Otus Eurobonds Liquidity"

  // 1. batch job (calculate bonds liquidity metrics from bonds quotes archive)

  val nowHour = CalcAnchorDate.calcHour()
  logger.info("nowHour: [" + nowHour + "]")
  val todayDate = CalcAnchorDate.calcDate(nowHour)
  logger.info("anchorDate: [" + todayDate + "]")

  val (exchangeQuotesFilePath, otcQuotesFilePath, eurobondsFilePath, outputFolder, outputFolderCsv) = (
    "/shara/sergei_baranov_" + todayDate + "/quotes_" + todayDate + ".csv",
    "/shara/sergei_baranov_" + todayDate + "/quotes_mp_month_raw_" + todayDate + ".csv",
    "/shara/sergei_baranov_" + todayDate + "/bonds_" + todayDate + ".csv",
    "/shara/liquidity/parquet_" + todayDate + "/",
    "/shara/liquidity/csv_" + todayDate + "/"
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
  Runner.run(conf, exchangeQuotesFilePath, otcQuotesFilePath, eurobondsFilePath, outputFolder, outputFolderCsv, needStreamJob)
}

object Runner extends App with LazyLogging {
  def run(conf: SparkConf,
          exchangeQuotesFilePath: String,
          otcQuotesFilePath: String,
          eurobondsFilePath: String,
          outputFolder: String,
          outputFolderCsv: String,
          needStreamJob: Boolean = false
         ): Unit = {

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

    val bondsLiquidityMetrics = LiquidityJobber.getLiquidityMetrics(exchangeQuotes, otcQuotes, bondsList)

    bondsLiquidityMetrics.show(20)
    bondsLiquidityMetrics.printSchema()

    bondsLiquidityMetrics.coalesce(1)
      .write.format("parquet").save(outputFolder)

    bondsLiquidityMetrics.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true").save(outputFolderCsv)

    //
    bondsLiquidityMetrics.unpersist()
    bondsLiquidityMetrics.sparkSession.stop()

    //

    if (false && !needStreamJob) {
      logger.info("Need no stream consumer before 10:00 MSK")
      sys.exit()
    }

    // 2. stream structured job (enrich bonds quotes stream with liquidity metrics)

    // вот тут он будет работать
    // до бесконечности
    /**
     * @TODO условия выхода в jobber-е
     * @TODO как получать от него выход с некоторой периодичностью?
     * Как гасить его отсюда?
     * разобраться
     */
    QuotesStreamJobber.enrichLiquidityWithIntradayQuotes(outputFolderCsv)
  }
}
