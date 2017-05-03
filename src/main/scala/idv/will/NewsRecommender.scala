package idv.will

import java.util.Scanner

import idv.will.model.NewsBean
import idv.will.util.{ElasticsearchClient, HBaseClient, HBaseWordBaseHelper, StringSeqUtils}
import org.slf4j.LoggerFactory


object NewsRecommender {

  val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val scanner=new Scanner(System.in)
    var content=""
    while(scanner.hasNextLine){
        val line=scanner.nextLine()
      if(line.trim.equals("!!!")){
        val list = search(content)
        list.foreach(x => LOG.info(x.id + " : " + x.title + " : " + x.content))
        content=""
      }
      content=content+"\n"+line
    }
  }

  def search(content: String): List[NewsBean] = {
    val wcMap = StringSeqUtils.wordCount(content)
      .toSeq
      .map(x => {
        val wordBase = HBaseWordBaseHelper.get(x._1)
        val score = if(wordBase.isDefined) {
          x._2.toDouble/wordBase.get.toDouble
        } else {
          0
        }
        (x._1, score)
    }).sortBy(_._2)(Ordering[Double].reverse).take(NewsCrawlerExecutor.KEYWORDS_NUM)
    LOG.info(wcMap.toString)
    val newsIds = ElasticsearchClient.queryRelevantNewsIds(wcMap.toMap.keys.toArray)
      .toSeq.sortBy(_._2)(Ordering[Long].reverse).map(_._1).toArray
    ElasticsearchClient.queryNews(newsIds)
  }

}
