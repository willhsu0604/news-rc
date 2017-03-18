package idv.will

import idv.will.model.NewsBean
import idv.will.util.{ElasticsearchClient, HBaseClient, HBaseWordBaseHelper, StringSeqUtils}
import org.slf4j.LoggerFactory


object NewsRecommender {

  val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val content = "回歸倒數中！林書豪隊內訓練狀況佳　最快25日歸隊▲林書豪。（圖／達志影像／美聯社） 記者潘泓鈺／綜合報導 本季屢遭左大腿筋傷勢侵襲的林書豪即將回歸。根據《紐約郵報》報導，林書豪15日\\\n        進行完整的隊內訓練，總教練亞特金森（Kenny Atkinson）認為狀況良好，預計明星週過後25日作客金塊重返球場。 籃網靈魂控衛林書豪去年底第二\\\n        度弄傷左大腿筋，在即將回歸的時刻，卻意外於上個月23日復健過程惡化，延遲歸隊日。本月初林書豪展開部分隊內訓練以及賽前練投，15日首度完成全套隊內訓練\\\n        ，且總教練亞特金森認為狀況十分理想，非常有可能在明星週過後25日作客金塊順利出戰。 「他幾乎所有項目都完成了，而且還多做了一些。」亞特金森在訓練結束\\\n        後說道，「林書豪狀況看起來不錯，投籃手感也保持的很好。他還要再休息將近10天，明星賽過後應該就能如期出戰」。林書豪談到自己的表現表示，「我打得並不理\\\n        想，但還保有競爭力！」 亞特金森補充道，「如果林書豪能順利回歸，對球隊絕對有很大的幫助。在這段期間林書豪的經驗談，幫助隊友，他的經驗正是我們需要的，\\\n        尤其這陣子籃網一勝難求。」 ▲籃網續寫隊史連敗紀錄。（圖／達志影像／美聯社） 籃網11月中陷入7連敗低潮，12月底至1月中創下搬遷布魯克林以來最長的\\\n        11連敗。1月21日拿鵜鶘止血後，又慘寫13連敗再創隊史紀錄，2017年目前僅獲1勝，剩16日對決公鹿的機會，能避免成為歷史第9支明星賽前個位數勝場\\\n        球隊。 截自13日，籃網場均攻下105.1分失114分，平均淨勝分-8.8分為聯盟最差，在自家主場平均要輸10.8分，是聯盟最不會打主場的球隊。目前\\\n        的9勝46敗勝率僅16.3%，排名歷史第12差。 ▲羅培茲恐在223交易大限前離隊。（圖／達志影像／美聯社） 儘管球隊將羅培茲（Brook Lopez）\\\n        擺上貨價，且希望在223交易大限前完成交易，不過總管馬克斯（Sean Marks）還是希望能看看林書豪與他聯手能達到什麼高度，畢盡林書豪本季因傷只出\\\n        賽12場，無法完整檢驗「Brook-Lin」組合。 ►接收更多精彩賽事，歡迎加入《ETtoday運動雲》粉絲團 ★圖片為版權照片，由達志影像供《ETtoday東\\\n        森新聞雲》專用，任何網站、報刊、電視台未經達志影像許可，不得部分或全部轉載！ WBC經典賽最後誰奪冠?39場賽事直播都在ETtoday APP 留言"
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
