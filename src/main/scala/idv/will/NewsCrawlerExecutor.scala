package idv.will

import idv.will.model.KeywordBean
import idv.will.util._
import org.slf4j.LoggerFactory
import idv.will.util.HBaseClient._

import scala.collection.mutable

object NewsCrawlerExecutor {

  val LOG = LoggerFactory.getLogger(getClass)

  val KEYWORDS_NUM = 100

  def main(args: Array[String]): Unit = {
    var minusDaysOffset = 30
    while(minusDaysOffset >= 0) {
      val map = mutable.Map[String, KeywordBean]()
      val newNewsIdMap = mutable.Map[String, String]()
      val newsList = NewsLoader.load(1000, minusDaysOffset)
      val keywordMap = newsList.map(news => {
        val wcMap = StringSeqUtils.wordCount(news.title + news.content)
        if(HBaseNewsHelper.isNewId(news.id)) {
          LOG.info("News with id [" + news.id + "] is new one")
          newNewsIdMap += (news.id -> "")
          for((key, value) <- wcMap) {
            val keywordBean = new KeywordBean()
            keywordBean.keyword = key
            keywordBean.score = if(!map.contains(key)) {
              value
            } else {
              map.get(key).get.score + value
            }
            map += (key -> keywordBean)
          }
        } else {
          LOG.info("News with id [" + news.id+ "] is already parsed")
        }
        val keywordBeanList = wcMap.toSeq.map(x => {
          new KeywordBean(news.id + "-" + x._1, news.id, x._1, x._2)
        })
        (news.id, keywordBeanList)
      })

      val sumMap = map.toSeq.map(e => {
        val value = if(HBaseWordBaseHelper.get(e._1).isEmpty) 0 else HBaseWordBaseHelper.get(e._1).get
        val keywordBean = e._2
        (e._1, keywordBean.score + value)
      }).toMap
      LOG.info("newNewsIdMap.size(): [" + newNewsIdMap.size + "]")
      LOG.info("Calculated sumMap.size(): [" + sumMap.size + "]")
      LOG.info("Starting to write data to database")
      writeToHBase(TABLE_NEWS, DEFAULT_FAMILY, QUALIFIER_NEWS_TAG, newNewsIdMap.toMap.asInstanceOf[Map[String, Any]], true)
      writeToHBase(TABLE_WORD_BASE, DEFAULT_FAMILY, QUALIFIER_WORD_BASE, sumMap.asInstanceOf[Map[String, Any]], true)

      val keywordBeanList = keywordMap.flatMap(x => {
        x._2.map(k => {
          val wordBase = HBaseWordBaseHelper.get(k.keyword)
          k.score = if(wordBase.isDefined) {
            k.score/wordBase.get
          } else {
            1.0
          }
          k
        }).sortBy(_.score)(Ordering[Double].reverse).take(KEYWORDS_NUM)
      })

      LOG.info("newsList.size(): [" + newsList.size + "]")
      LOG.info("keywordBeanList.size(): [" + keywordBeanList.size + "]")
      LOG.info("Starting to write data to Elasticsearch")
      ElasticsearchClient.bulkLoad(ElasticsearchClient.INDEX_NEWS_RC, ElasticsearchClient.TYPE_NEWS, newsList)
      ElasticsearchClient.bulkLoad(ElasticsearchClient.INDEX_NEWS_RC, ElasticsearchClient.TYPE_KEYWORD, keywordBeanList)

      minusDaysOffset -= 1
    }
  }

}
