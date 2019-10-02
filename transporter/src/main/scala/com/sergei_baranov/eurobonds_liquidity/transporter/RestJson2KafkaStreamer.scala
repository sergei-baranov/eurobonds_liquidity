package com.sergei_baranov.eurobonds_liquidity.transporter

import scala.concurrent.duration._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpMethods, HttpRequest, HttpResponse, StatusCodes}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.ByteString
import java.util.{Calendar, Properties, TimeZone}
import java.text.SimpleDateFormat
import java.util

import akka.NotUsed
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import com.typesafe.scalalogging._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.typesafe.scalalogging._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.native.Json
import org.json4s.DefaultFormats

/**
 * вот так вот я организовал интервальный опрос рест-а:
 * Actor делает http-запрос, разбирает его, пихает в топик поочерёдно котировки,
 * а потом выставляет сам себе акка-шедулером задачу повторить всё через 5 минут
 * (не знаю, делают ли так в жизни, но укопавшись в доках по akka как-то так я
 * только смог, с учётом того, что у меня не text/stream, а application/json
 * на той стороне)
 *
 * @param MyCurrentAuthenticator
 * @param producer
 */
class RestJson2KafkaStreamer(
                              val MyCurrentAuthenticator: MyAuthenticator,
                              val producer: KafkaProducer[String, String]
                            ) extends Actor with ActorLogging {

  import akka.pattern.pipe
  import context.dispatcher

  val reqUri = "https://ws.cbonds.info/services/json/get_tradings_realtime/?lang=eng"
  val reqBody = "{\"auth\":{\"login\":\"" + this.MyCurrentAuthenticator.getAuthUserName + "\"," +
    "\"password\":\"" + this.MyCurrentAuthenticator.getAuthPassword + "\"}," +
    "\"filters\":[],\"quantity\":{\"limit\":1000,\"offset\":0}," +
    "\"sorting\":[{\"field\":\"emission_id\",\"order\":\"asc\"}]}"

  final implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))

  val http = Http(context.system)

  override def preStart() = {
    http.singleRequest(HttpRequest(
      method = HttpMethods.POST,
      uri = this.reqUri,
      entity = HttpEntity(ContentTypes.`application/json`, this.reqBody)
    ))
      .pipeTo(self)
  }

  def receive = {
    case "NextIteration" => this.preStart()

    case HttpResponse(StatusCodes.OK, headers, entity, _) =>
      entity.dataBytes.runFold(ByteString(""))(_ ++ _).foreach { body =>
        val jsonStr = body.utf8String
        log.info("Got response, body: " + jsonStr.length + " characters")
        val quotesItems = this.parseJsonStr(jsonStr)
      }
      context.system.scheduler.scheduleOnce(300 seconds, self, "NextIteration")

    case resp @ HttpResponse(code, _, _, _) =>
      log.info("Request failed, response code: " + code)
      resp.discardEntityBytes()
      // всё равно поставим задачу, чтобы цикл не прерывался
      context.system.scheduler.scheduleOnce(300 seconds, self, "NextIteration")
  }

  /**
   * {
   *   "count":387, "total":387, "limit":1000, "offset":0, "exec_time":1.0425,
   *   "items":[
   *     {
   *       "id":"20201","agreement_number":"0",
   *       "buying_quote":"168.6","date":"2019-10-01","emission_id":"238",
   *       "overturn":"0","selling_quote":"170.31",
   *       "trading_ground_id":"20",
   *       "update_time":"2019-10-01T14:32:55",
   *       "volume":"0","volume_money":"0"
   *     },
   *     ...
   *     {
   *       "id":"233685","agreement_number":"0",
   *       "buying_quote":"99.51","date":"2019-10-01","emission_id":"610967",
   *       "overturn":"0","selling_quote":"99.93",
   *       "trading_ground_id":"255",
   *       "update_time":"2019-10-01T14:18:19",
   *       "volume":"0","volume_money":"0"
   *     }
   *   ],
   *   "meta":{
   *     "cms_full_gen_time":1.0434000000000001,
   *     "user_id":105,
   *     "lang":"eng",
   *     "strict_mode":1,
   *     "performLogging":-1
   *   }
   * }
   *
   * @param jsonStr
   */
  def parseJsonStr(jsonStr: String): Unit = {
    val jsonMap = this.jsonStrToMap(jsonStr)
    val count = jsonMap.getOrElse("count", 0).asInstanceOf[BigInt]
    log.info("quotes: " + count + " items")
    val itemsAny = jsonMap.get("items")
    log.info("itemsAny type: " + itemsAny.getClass.getName)
    val itemsColl = itemsAny.get
    log.info("itemsColl type: " + itemsColl.getClass.getName)
    itemsColl match {
      case a: List[Any] => {
        for (item <- itemsColl.asInstanceOf[List[Any]]) {
          //log.info("item type: " + item.getClass.getName)
          item match {
            case b: Map[String, Any] => {
              val itemValid = item.asInstanceOf[Map[String, Any]]
              val eventData = Map(
                "id"            -> itemValid.getOrElse("id",                0),
                "bond"          -> itemValid.getOrElse("emission_id",       0),
                "bid"           -> itemValid.getOrElse("buying_quote",      0),
                "ask"           -> itemValid.getOrElse("selling_quote",     0),
                "tg"            -> itemValid.getOrElse("trading_ground_id", 0),
                "date"          -> itemValid.getOrElse("date",              "1970-01-01"),
                "update_ts"     -> itemValid.getOrElse("update_time",       "1970-01-01T00:00:00"),
                "volume"        -> itemValid.getOrElse("volume",            0),
                "volume_money"  -> itemValid.getOrElse("volume_money",      0),
                "overturn"      -> itemValid.getOrElse("overturn",          0)
              )
              val data = new ProducerRecord[String, String](
                "eurobonds-quotes-topic",
                Json(DefaultFormats).write(eventData)
              )
              this.producer.send(data)
              log.info("event sent ("+ itemValid.getOrElse("id", 0) +")")
            }
            case _ => log.info("item bad match")
          }
        }
        //this.producer.flush()
      }
      case _ => log.info("itemsColl bad match")
    }
    /*
    var a = 0
    while ( {
      a < count
    }) {
      val item = itemsAny(a)
      log.info("item type [" + a + "]: " + item.getClass.getName)
      a = a + 1
    }

     */
    /*
    for (itemsColl: Array[Any] <- itemsAny if !itemsColl.isEmpty) {
      log.info("itemsColl type: " + itemsColl.getClass.getName)
      for (item <- itemsColl) {
        log.info("itemsColl type: " + item.getClass.getName)
      }
    }
    */
  }
  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats = org.json4s.DefaultFormats

    parse(jsonStr).extract[Map[String, Any]]
  }

}