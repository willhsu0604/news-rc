package idv.will

import idv.will.model.NewsBean
import idv.will.util.{HBaseWordBaseHelper, StringSeqUtils}
import org.slf4j.LoggerFactory


object NewsRecommender {

  val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    HBaseClient.setup()
    val content = "IMF總裁譴責爆炸案是「懦弱行為」　陸軍警犬進駐搜..▲國際貨幣組織發生爆炸案，法國警方派警犬進駐。（圖／路透社） 記者林彥臣／綜合報導 巴黎世界銀行兼國際貨幣基金組織驚傳爆炸案，一人打\\\n        開信封後啟動了引爆裝置，目前確認一人輕傷，由於時間地點皆十分敏感，法國當局不敢大意，已派出陸軍以及警犬在現場搜索，國際貨幣組織總裁拉加德（Christine\\\n        \\ Lagarde）也譴責這是懦弱行為。 ▲國際貨幣基金組織現任總裁拉加德，譴責爆炸案是懦弱行為。（圖／達志影像／美聯社） 國際貨幣組織（International\\\n        \\ Monetary Fund，IMF）總裁拉加德（Christine Lagarde）在爆炸案後發表聲明表示，這次的攻擊事件，「是一個懦弱的暴力行\\\n        為（cowardly act of violence）」，造成了我們的工作人員受傷，基金組織會按照原定計畫完成未來的任務，之後會與法國當局密切合作，\\\n        調查這起事件，並維護工作人員的安全。 國際貨幣組織方面的知情人士透露，當天上午有接獲一通恐嚇電話，但是無法確定是否與這次的爆炸案件有關，有人在辦公室\\\n        裡打開信封之後，就發生了爆炸。警察局長卡多（Michel Cadot）表示，這是一件相當土製的爆裂物。（It was something that\\\n        \\ was fairly homemade） 警方已在事發現場附近實施封鎖，包括耶拿大街（Avenue d'Iena）以及香榭麗舍大道，警方目前還無\\\n        法確定寄送炸彈的兇手，以及行兇動機，法國總統歐蘭德對這次事件表示，「將盡全力對這起事件負責」（would do all they could to\\\n        \\ find those responsible for the incident.） 據了解，這枚信封炸彈的威力，大約只相當於一枚大型爆竹，只造成\\\n        一人受到輕傷，但是因為距離法國總統大選只有6週的時間，再加上前一天（15日），德國財政部長蕭伯樂（Wolfgang Schaeuble），收到來自希\\\n        臘激進武裝分子組織的寄來的疑似信封炸彈的信函，因此法國當局對這起事件格外重視。 ▼法國陸軍也進駐IMF周邊進行戒備。（圖／美聯社）"
    val list = search(content)
    list.foreach(x => LOG.info(x.id + " : " + x.title + " : " + x.content))
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
