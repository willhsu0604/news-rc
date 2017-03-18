package idv.will.util

import org.jsoup.Jsoup

import scala.collection.mutable
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import idv.will.model.NewsBean
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object NewsLoader {

  val LOG = LoggerFactory.getLogger(getClass)

  private val NEWS_LIST_URL = "http://www.ettoday.net/show_roll.php"
  private val NEWS_COTENT_URL_PRFIX = "http://www.ettoday.net"

  def load(pullNewsMaxAmount: Int, minusDaysOffset: Int): List[NewsBean] = {
    var list = mutable.ListBuffer[NewsBean]()
    val dateStr = LocalDate.now().minusDays(minusDaysOffset).format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    var count = 0
    var offsetIndex = 0

    while(count < pullNewsMaxAmount) {
      val map = Map[String, String](
        "offset" -> String.valueOf(offsetIndex),
        "tPage" -> "1",
        "tFile" -> (dateStr + ".xml"),
        "tOt" -> "0",
        "tSi" -> "100"
      ).asJava

      try {
        LOG.info("Reading news list from page [" + offsetIndex + "]")
        val doc = Jsoup.connect(NEWS_LIST_URL)
          .header("Content-Type", "application/x-www-form-urlencoded")
          .header("charset", "UTF-8")
          .data(map)
          .post()
        val elements = doc.getElementsByTag("h3")
        if(elements.isEmpty) {
          count += 1
        } else {
          val it = elements.iterator()
          while(it.hasNext) {
            val el = it.next()
            val content_url_suffix = el.child(0).attr("href")
            val title = el.text()
            val newsId = content_url_suffix.substring(content_url_suffix.lastIndexOf("/") + 1, content_url_suffix.lastIndexOf("."))
            val contentDoc = Jsoup.connect(NEWS_COTENT_URL_PRFIX + content_url_suffix)
              .timeout(10 * 1000)
              .get()
            if(!contentDoc.getElementsByClass("story").isEmpty) {
              val storyEl = contentDoc.getElementsByClass("story").get(0)
              val content = Jsoup.parse(storyEl.html()).text()
              val newsBean = new NewsBean(newsId, title, content)
              list += newsBean

            }
            count += 1
          }
        }
      } catch {
        case ignored: Exception => {
          LOG.warn("Exception occured when running on offset = " + offsetIndex)
          count += 1
        }
      }
      LOG.info("Current try to pull attempt times: [" + count + "]")
      offsetIndex += 1
    }
    list.toList
  }

}
