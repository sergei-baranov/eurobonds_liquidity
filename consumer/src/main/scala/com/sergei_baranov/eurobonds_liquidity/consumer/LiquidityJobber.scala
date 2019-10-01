package com.sergei_baranov.eurobonds_liquidity.consumer

import java.util.Calendar

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{avg, collect_list, concat_ws, count, first, split, sum}
import org.apache.spark.sql.types._

object LiquidityJobber {
  /**
   *
   */
  def getLiquidityMetrics(
                           exchangeQuotes: DataFrame,
                           otcQuotes: DataFrame,
                           bondsList: DataFrame
                         ): DataFrame = {

    var Cal = Calendar.getInstance()
    val tsStart = Cal.getTime().getTime

    val spark = SparkSession.getActiveSession.get
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // 1. внебиржевые котировки приходят bid и ask построчно, пересобираем их в колонки
    val otcQuotesCols =
      otcQuotes.where(
        $"quote_type" === 1
        && $"bond".isNotNull
        && $"org".isNotNull
        && $"date".isNotNull
        && $"price".isNotNull
        && $"price" > 0
      ).as("otcQuotesL")
      .join(
        otcQuotes.where(
          $"quote_type" === 2
            && $"bond".isNotNull
            && $"org".isNotNull
            && $"date".isNotNull
            && $"price".isNotNull
            && $"price" > 0
        ).as("otcQuotesR"),
        (
          $"otcQuotesR.bond" === $"otcQuotesL.bond"
          && $"otcQuotesR.org" === $"otcQuotesL.org"
          && $"otcQuotesR.date" === $"otcQuotesL.date"
        ),
        "inner"
      )
      .select(
        $"otcQuotesL.bond".as("bond").cast(LongType),
        $"otcQuotesL.org".as("org").cast(LongType),
        $"otcQuotesL.date".as("date").cast(TimestampType),
        $"otcQuotesL.price".as("bid").cast(DoubleType),
        $"otcQuotesR.price".as("ask").cast(DoubleType)
      )
    val otcSchema = new StructType()
      .add(StructField("bond", LongType, false))
      .add(StructField("org", LongType, false))
      .add(StructField("date", TimestampType, false))
      .add(StructField("bid", DoubleType, false))
      .add(StructField("ask", DoubleType, false))
    val otcQuotesStrict = spark.createDataFrame(otcQuotesCols.rdd, otcSchema)
    otcQuotesCols.unpersist()
    otcQuotes.unpersist()
    //otcQuotesStrict.show(20)
    //otcQuotesStrict.printSchema()

    // 2. на биржевые котировки просто чистим и накидываем явную схему

    val exchQuotes = exchangeQuotes
    .filter(
        $"bond".isNotNull
        && $"tg".isNotNull
        && $"date".isNotNull
        && $"bid".isNotNull
        && $"ask".isNotNull
        && $"tg".notEqual(327)
        && $"tg".notEqual(381)
    )
    .select(
      $"bond".as("bond").cast(LongType),
      $"tg".as("tg").cast(LongType),
      $"date".as("date").cast(TimestampType),
      $"bid".as("bid").cast(DoubleType),
      $"ask".as("ask").cast(DoubleType)
    )
    val exchSchema = new StructType()
      .add(StructField("bond", LongType, false))
      .add(StructField("tg", LongType, false))
      .add(StructField("date", TimestampType, false))
      .add(StructField("bid", DoubleType, false))
      .add(StructField("ask", DoubleType, false))
    val exchQuotesStrict = spark.createDataFrame(exchQuotes.rdd, exchSchema)
    exchQuotes.unpersist()
    exchangeQuotes.unpersist()
    //exchQuotesStrict.show(20)
    //exchQuotesStrict.printSchema()

    // 3. то же со списком евробондов
    // val bondsListBroadcast = broadcast(bondsList)
    val bondsListStrictClean = bondsList
      .filter(
        $"id".isNotNull
        && $"isin_code".isNotNull
      )
      .select(
        $"id".as("id").cast(LongType),
        $"isin_code".as("isin").cast(StringType),
        $"date_of_end_placing".as("date_of_end_placing").cast(TimestampType),
        $"maturity_date".as("maturity_date").cast(TimestampType),
        $"usd_volume".as("usd_volume").cast(DoubleType)
      )
    val bondsSchema = new StructType()
      .add(StructField("id", LongType, false))
      .add(StructField("isin", StringType, false))
      .add(StructField("date_of_end_placing", TimestampType, true))
      .add(StructField("maturity_date", TimestampType, true))
      .add(StructField("usd_volume", DoubleType, true))
    val bondsListStrict = spark.createDataFrame(bondsListStrictClean.rdd, bondsSchema)
    bondsListStrictClean.unpersist()
    bondsList.unpersist()
    //bondsListStrict.show(20)
    //bondsListStrict.printSchema()

    // 4. bid_ask_spread_relative_median, bid_ask_spread_relative_rank
    /*
    bid_ask_spread_relative_median:
    считаем относительный bid-ask спред по каждой записи,
    на каждую дату считаем медиану bid-ask спреда по биржам для бумаги,
    и далее - медиану ото всех дат для бумаги
    bid_ask_spread_relative_rank:
    сортируем бонды desc по bid_ask_spread_relative_median
    и делим позицию бумаги на общее количество бумаг
    */
    // считаем относительный bid-ask спред по каждой записи
    val dfBASpread = exchQuotesStrict
      .withColumn("ba_spread", (($"ask" - $"bid")/(($"ask" + $"bid")/2)))
    //dfBASpread.orderBy($"bond".asc).show(20)

    // на каждую дату считаем медиану bid-ask спреда по биржам для бумаги
    dfBASpread.createOrReplaceTempView("BASpread")
    val dfMedianByBondAndDate = spark.sql("""SELECT bond, `date`, percentile_approx(ba_spread, 0.5, 100) AS median_of_tgs
FROM BASpread GROUP BY bond, `date`""")
    dfBASpread.unpersist()
    //dfMedianByBondAndDate.orderBy($"bond".asc).show(20)

    // и далее - медиану ото всех дат для бумаги
    dfMedianByBondAndDate.createOrReplaceTempView("MedianByBondAndDate")
    val dfBASpreadRelativeMedian = spark.sql("""SELECT bond, percentile_approx(median_of_tgs, 0.5, 100) AS bid_ask_spread_relative_median
FROM MedianByBondAndDate GROUP BY bond""")
    val cntDfBASpreadRelativeMedian = dfBASpreadRelativeMedian.count()
    // и rank по ней
    val winRnkBaspread = Window.orderBy($"bid_ask_spread_relative_median".desc)
    val dfBASpreadRank = dfBASpreadRelativeMedian
      .sort($"bid_ask_spread_relative_median".desc)
      .withColumn("total", lit(cntDfBASpreadRelativeMedian))
      .withColumn(
        "current",
        (row_number() over winRnkBaspread)
      )
      .withColumn(
        "bid_ask_spread_relative_rank",
        ($"current"/$"total")
      )
      .select($"bond", $"bid_ask_spread_relative_median", $"bid_ask_spread_relative_rank")
    dfBASpreadRelativeMedian.unpersist()
    dfMedianByBondAndDate.unpersist()
    //dfBASpreadRank.orderBy($"bond".asc).show(20)

    // 5. 6. 7. 8.
    /** @TODO ещё 4 метрики */

    //
    val bondsLiquidityMetrics = bondsListStrict.as("L")
        .join(
          dfBASpreadRank.as("R"),
          $"R.bond" === $"L.id",
          "inner"
        )
        .select($"id", $"isin", $"date_of_end_placing", $"maturity_date", $"usd_volume",
          $"bid_ask_spread_relative_median", $"bid_ask_spread_relative_rank")
        .orderBy($"bid_ask_spread_relative_median".desc)
    dfBASpreadRelativeMedian.unpersist()

    //
    Cal = Calendar.getInstance()
    val tsFin = Cal.getTime().getTime
    val tsDelta = ((tsFin - tsStart)/1000)
    println("tsStart: " + tsStart + ", tsFin: " + tsFin + ", tsDelta: " + tsDelta)

    bondsLiquidityMetrics
  }
}
