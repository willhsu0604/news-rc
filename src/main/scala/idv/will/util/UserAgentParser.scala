package idv.will.util

import java.io.{File, PrintWriter}
import java.net.SocketTimeoutException
import java.util.concurrent.atomic.AtomicInteger

import org.apache.commons.io.FileUtils
import org.jsoup.Jsoup

import scala.io.Source

object UserAgentParser {

  case class UserAgentDevice(userAgent: String, brand: String, model: String)

  val total = new AtomicInteger(0)

  def main(args: Array[String]): Unit = {
    val chunkSize = 16 * 1024
    val iterator = Source.fromFile("/Users/willhsu/Downloads/dora-ettoday_distinct_user_agent-20171112_000000.tsv").getLines.grouped(chunkSize)
    val directory = new File("/tmp/user-agent-device")
    if(directory.isDirectory) {
      FileUtils.deleteDirectory(directory)
    }
    directory.mkdir()
    iterator.zipWithIndex.foreach{x =>
      val (lines, index) = x
      val pw = new PrintWriter(new File(directory, index + ".tsv"))
      new Thread(new Crawler(index, lines, pw)).start()
    }
  }

  class Crawler(index: Int, lines: Seq[String], pw: PrintWriter) extends Runnable {

    override def run(): Unit = {
      lines.zipWithIndex.foreach(x => {
        val (line, lineIndex) = x
        var isFinished = false
        while(!isFinished) {
          try {
            val url = "https://tools.scientiamobile.com/?user-agent-string=" + line
            val doc = Jsoup.connect(url).get()
            val text = doc.select("span.monospace").eq(1).text()
            val device = text.replace(" Link to this result", "")
            val userAgentDevice = UserAgentDevice(line, device.substring(0, text.indexOf(" ")), device.substring(text.indexOf(" ") + 1))
            pw.write(userAgentDevice.userAgent + "\t" + userAgentDevice.brand + "\t" + userAgentDevice.model + "\n")
            total.incrementAndGet()

            if(total.get%100 == 0) {
              println(total + " record is executed.")
            }
            isFinished = true
          } catch {
            case e: SocketTimeoutException => {
              println("File [" + index + "] with line [" + lineIndex + "] error occurred: " + e)
            }
            case e => {
              println("File [" + index + "] with line [" + lineIndex + "] error occurred, ignore this record " + e)
              isFinished = true
            }
          }
        }
      })
      pw.flush()
    }

  }

}
