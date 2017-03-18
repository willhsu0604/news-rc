package idv.will.util

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader}
import idv.will.HBaseClient._

object HBaseNewsHelper {

  val cache = CacheBuilder.newBuilder().maximumSize(100000).expireAfterWrite(30, TimeUnit.SECONDS)
    .build(new CacheLoader[String, Option[String]]() {
      override def load(newsId: String): Option[String] = {
        try {
          Option(getValue[String](TABLE_NEWS, newsId, DEFAULT_FAMILY, QUALIFIER_NEWS_TAG))
        } catch {
          case ignored: NullPointerException => {
            None
          }
        }
      }
    })

  def isNewId(newsId: String): Boolean = {
    if(cache.get(newsId).isEmpty) {
      true
    } else {
      false
    }
  }

}
