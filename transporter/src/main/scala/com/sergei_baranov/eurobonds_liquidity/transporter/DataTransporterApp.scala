package com.sergei_baranov.eurobonds_liquidity.transporter

import java.util.{Calendar, Properties, TimeZone}
import java.text.SimpleDateFormat

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{RestartSource, Source}
import com.typesafe.scalalogging._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.duration._
import java.net.URL
import java.io.File
import org.apache.commons.io.FileUtils
import java.net.Authenticator
import java.net.PasswordAuthentication

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
    var dtMillis = Cal.getTime()
    //logger.info("dtMillis: " + dtMillis)
    val todayDate = (format.format(dtMillis))

    todayDate
  }
}

// далее по коду Authenticator.setDefault принимает объект,
// который видимо должен вернуть креденшлы в методе getPasswordAuthentication
class MyAuthenticator(val username: String, val password: String) extends Authenticator {
  var authUserName: String = username;
  var authPassword: String = password;

  override protected def getPasswordAuthentication = new PasswordAuthentication(this.authUserName, this.authPassword.toCharArray)

  def getAuthUserName: String = this.authUserName

  def getAuthPassword: String = this.authPassword
}

object DataTransporterApp extends App with StrictLogging {

  "touch /shara/somefile.txt" !!

  // запрашиваем креденшлы для сервисов CBonds
  /*
  println("enter cbonds.info services login for sergei_baranov account\n")
  var cbonds_username = scala.io.StdIn.readLine()
  println("enter cbonds.info services password for sergei_baranov account\n")
  var cbonds_password = scala.io.StdIn.readLine()
   */
  val (cbonds_username, cbonds_password) = (args(1), args(2)) // see Dockerfile

  logger.info("cbonds_username: " + cbonds_username)
  logger.info("cbonds_password: " + cbonds_password)

  // вот так вот в жабе пробиваем аутентификацию
  val MyCurrentAuthenticator = new MyAuthenticator(cbonds_username, cbonds_password)
  logger.info("credentials: [" + MyCurrentAuthenticator.toString + "]")
  logger.info("MyCurrentAuthenticator getAuthUserName: " + MyCurrentAuthenticator.getAuthUserName)
  logger.info("cMyCurrentAuthenticator getAuthPassword: " + MyCurrentAuthenticator.getAuthPassword)
  Authenticator.setDefault(MyCurrentAuthenticator)

  // Сначала скачиваем исходные данные для вычисления метрик ликвидности
  logger.info("Download data for batch jobber")
  // Выкачиваем zip со списком евробондов россии и снг и архивами котировок (биржевые и внебиржевые).
  // соглашение по именованию на источнике: username_yyyMMdd.zip
  val nowHour = CalcAnchorDate.calcHour()
  logger.info("nowHour: [" + nowHour + "]")
  val todayDate = CalcAnchorDate.calcDate(nowHour)
  logger.info("anchorDate: [" + todayDate + "]")

  // FileUtils.copyURLToFile всё делает, используя Authenticator
  val zipName = "sergei_baranov_" + todayDate + ".zip"
  val zipPathRemote = "https://database.cbonds.info/unloads/sergei_baranov/archive/" + zipName
  val tmpDir = "/shara/tmp/"
  val tmpZipPath = tmpDir + zipName
  val tmpUnzippedDir = "/shara/sergei_baranov_" + todayDate + "/"

  logger.info("zipPathRemote: [" + zipPathRemote + "]")
  logger.info("tmpZipPath: [" + tmpZipPath + "]")
  logger.info("tmpUnzippedDir: [" + tmpUnzippedDir + "]")

  /** @TODO обработка исключений */

  "rm -f " + tmpZipPath !!

  "rm -rf " + tmpUnzippedDir !!

  FileUtils.copyURLToFile(new URL(zipPathRemote), new File(tmpZipPath))
  logger.info("copyURLToFile:" + "ok")

  "unzip " + tmpZipPath + " -d " + tmpUnzippedDir !!

  val lsDir = "ls -lt " + tmpUnzippedDir !!

  logger.info(lsDir)

  "rm -f " + tmpZipPath !!

  "rm -rf " + tmpDir !!

  val lsDir2 = "ls -alt /shara" !!

  logger.info(lsDir2)

  // теперь организовываем поток в кафку
  // но не ранее 10-ти часов по Мск
  if (nowHour < 10) {
    logger.info("Need no stream transporter before 10:00 MSK")
    sys.exit(0)
  }

  /*
  logger.info("Initializing FlowProducer, sleeping for 30 seconds to let Kafka startup")
  Thread.sleep(300)

  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()

  import akka.http.scaladsl.unmarshalling.sse.EventStreamUnmarshalling._
  import system.dispatcher

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

  var msgCounter = 0

  val restartSource = RestartSource.withBackoff(
    minBackoff = 3.seconds,
    maxBackoff = 10.seconds,
    randomFactor = 0.2
  ) { () =>
    Source.fromFutureSource {
      Http().singleRequest(Get("https://stream.wikimedia.org/v2/stream/recentchange"))
        .flatMap(Unmarshal(_).to[Source[ServerSentEvent, NotUsed]])
    }
  }

  restartSource.runForeach(elem => {
    msgCounter += 1

    val data = new ProducerRecord[String, String]("eurobonds-quotes-topic", elem.data)

    producer.send(data)

    if (msgCounter % 100 == 0) {
      logger.info(s"New messages came, total: $msgCounter messages")
    }

  })

   */
}
