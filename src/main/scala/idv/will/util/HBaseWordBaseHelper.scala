package idv.will.util

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader}
import HBaseClient.{DEFAULT_FAMILY, QUALIFIER_WORD_BASE, TABLE_WORD_BASE, getValue}


object HBaseWordBaseHelper {

  val cache = CacheBuilder.newBuilder().maximumSize(100000).expireAfterWrite(30, TimeUnit.SECONDS)
    .build(new CacheLoader[String, Option[Long]]() {
      override def load(key: String): Option[Long] = {
        try {
          Option(getValue[Long](TABLE_WORD_BASE, key, DEFAULT_FAMILY, QUALIFIER_WORD_BASE))
        } catch {
          case ignored: NullPointerException => {
            None
          }
        }
      }
    })

  def get(keyword: String): Option[Long] = {
    cache.get(keyword)
  }

}
