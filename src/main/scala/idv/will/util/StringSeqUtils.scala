package idv.will.util

import org.ansj.splitWord.analysis.NlpAnalysis

import scala.collection.mutable

object StringSeqUtils {

  def wordCount(s: String) : Map[String, Long] = {
    val it = NlpAnalysis.parse(s).iterator()
    val map = mutable.Map[String, Long]()
    while(it.hasNext) {
      val w = it.next().getName()
      if(!map.contains(w)) {
        map += (w -> 1)
      } else {
        map += (w -> (map.get(w).get + 1))
      }
    }
    map.toMap
  }

}
