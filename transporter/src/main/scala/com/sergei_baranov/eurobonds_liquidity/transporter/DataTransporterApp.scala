package com.sergei_baranov.eurobonds_liquidity.transporter

import java.util.{Calendar, Properties, TimeZone}
import java.text.SimpleDateFormat

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{RestartSource, Source}
import com.typesafe.scalalogging._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest, HttpResponse, ResponseEntity}

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import scala.concurrent.duration._
import java.net.URL
import java.io.File

import org.apache.commons.io.FileUtils
import java.net.Authenticator
import java.net.PasswordAuthentication

import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._

import sys.process._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.{ByteString, Timeout}

import akka.actor.ActorSystem

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
    var dtMillis = Cal.getTime()
    //logger.info("dtMillis: " + dtMillis)
    val todayDate = (format.format(dtMillis))

    todayDate
  }
}

object DataTransporterApp extends App with StrictLogging {

  // запрашиваем креденшлы для сервисов CBonds
  val (cbonds_username, cbonds_password) = (args(1), args(2)) // see Dockerfile
  // и сохраняем их в объект
  val MyCurrentAuthenticator = new MyAuthenticator(cbonds_username, cbonds_password)
  Authenticator.setDefault(MyCurrentAuthenticator)

  // до 10-ти утра по МСК не надо работать стримы,
  // а данные для батчей брать, но на вчера
  val nowHour = CalcAnchorDate.calcHour()
  logger.info("nowHour: [" + nowHour + "]")
  // соответственно todayDate до 10-тичасов содержит вчерашнюю дату
  val todayDate = CalcAnchorDate.calcDate(nowHour)
  logger.info("anchorDate: [" + todayDate + "]")

  // Сначала скачиваем исходные данные для вычисления метрик ликвидности
  logger.info("Download data for batch jobber")
  val Stager4Batch = new Stage4Batch(MyCurrentAuthenticator, todayDate)
  Stager4Batch.mkJob()

  // теперь организовываем поток в кафку
  // но не ранее 10-ти часов по Мск
  if (false && nowHour < 10) {
    logger.info("Need no stream transporter before 10:00 MSK")
    sys.exit(0) /** @TODO как убить ещё и кафку? */
  }

  //
  logger.info("Initializing FlowProducer, sleeping for 30 seconds to let Kafka startup")
  Thread.sleep(300) // хотя в текущей рейлизации пока мы выкачивали csv-шки и гоняли их по диску..

  val props = new Properties()

  props.put("bootstrap.servers", "kafka:9092")
  props.put("client.id", "producer")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")
  props.put("metadata.max.age.ms", "10000")


  val producer = new KafkaProducer[String, String](props)
  producer.flush()

  logger.info("Kafka producer initialized")

  // Поток в Кафку мы получим из интрадей делейед - котировок
  // Это котировки с задержкой в несколько [десятков] минут
  // относительно времени сделки на бирже, но у нас могут появиться когда угодно
  // Берём их из веб-сервиса корпоративного с некоторой периодичностью
  // в виде json-а и определяем топик в кафку, из которого их будет ловить
  // AnalyticsConsumer
  implicit val system = ActorSystem("Main")
  system.actorOf(Props(new RestJson2KafkaStreamer(MyCurrentAuthenticator, producer)))
}
