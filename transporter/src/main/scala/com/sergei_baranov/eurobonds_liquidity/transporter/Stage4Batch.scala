package com.sergei_baranov.eurobonds_liquidity.transporter

import sys.process._
import java.io.File
import java.net.{Authenticator, PasswordAuthentication, URL}

import com.typesafe.scalalogging.StrictLogging
import org.apache.commons.io.FileUtils

// далее по коду Authenticator.setDefault принимает объект,
// который видимо должен вернуть креденшлы в методе getPasswordAuthentication
class MyAuthenticator(val username: String, val password: String) extends Authenticator {
  var authUserName: String = username;
  var authPassword: String = password;

  override protected def getPasswordAuthentication = new PasswordAuthentication(this.authUserName, this.authPassword.toCharArray)

  def getAuthUserName: String = this.authUserName

  def getAuthPassword: String = this.authPassword
}

/**
 * Забирает и складывает в файловой системе исходные данные для
 * расчётов метрик ликвидности (zip с 4-мя csv-шками)
 * Файлы потом используются как исходныен данные в AnalyticsConsumer
 *
 * @param MyCurrentAuthenticator
 * @param todayDate
 */
class Stage4Batch(MyCurrentAuthenticator: MyAuthenticator, todayDate: String) extends App with StrictLogging {
  def mkJob(): Unit = {
    // shara - я прописал такой volume во всех докерах
    "touch /shara/somefile.txt" !!

    // Выкачиваем zip со списком евробондов россии и снг и архивами котировок (биржевые и внебиржевые).
    // соглашение по именованию на источнике: username_yyyMMdd.zip
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
  }
}
