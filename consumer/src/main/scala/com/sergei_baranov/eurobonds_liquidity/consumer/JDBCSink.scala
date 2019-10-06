package com.sergei_baranov.eurobonds_liquidity.consumer

import java.sql._

import org.apache.spark.sql.ForeachWriter

/*
мнда, это, конечно, забавно
Не получается, да и не вариант. Оставляю на память.
*/

// https://docs.databricks.com/_static/notebooks/structured-streaming-etl-kafka.html
class  JDBCSink(url:String, user:String, pwd:String) extends ForeachWriter[(String, String, String, String, String, String, String, String, String, String, String)] {
  val driver = "com.mysql.cj.jdbc.Driver"
  var connection:Connection = _
  var statement:Statement = _

  def open(partitionId: Long,version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }

  def process(value: (String, String, String, String, String, String, String, String, String, String, String)): Unit = {
    val bond = value._1
    val isin = value._2
    val tg = value._3
    val date = value._4
    val volume_money = value._5
    val bid = value._6
    val ask = value._7
    val update_ts = value._8
    val baspread_rel_median = value._9
    val baspread_rel_rank = value._10
    val upsert_time = value._11
    statement.executeUpdate("""
      |INSERT INTO
      |  `test`.`quotes_with_liquidity_metrics`
      |SET
      |  `bond` = '${bond}',
      |  `isin` = '${isin}',
      |  `tg` = '${tg}',
      |  `date` = '${date}',
      |  `volume_money` = '${volume_money}',
      |  `bid` = '${bid}',
      |  `ask` = '${ask}',
      |  `update_ts` = '${update_ts}',
      |  `baspread_rel_median` = '${baspread_rel_median}',
      |  `baspread_rel_rank` = '${baspread_rel_rank}',
      |  `upsert_time` = '${upsert_time}'
      |ON DUPLICATE KEY UPDATE
      |  `isin` `= VALUES(`isin`),
      |  `volume_money` `= VALUES(`volume_money`),
      |  `bid` `= VALUES(`bid`),
      |  `ask` `= VALUES(`ask`),
      |  `update_ts` `= VALUES(`update_ts`),
      |  `baspread_rel_median` `= VALUES(`baspread_rel_median`),
      |  `baspread_rel_rank` `= VALUES(`baspread_rel_rank`),
      |  `upsert_time` `= VALUES(`upsert_time`)
    """)
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}