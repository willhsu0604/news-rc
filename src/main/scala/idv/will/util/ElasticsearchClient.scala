package idv.will.util

import java.net.{InetAddress, UnknownHostException}

import idv.will.model.{EsElement, EsElementNoId, EsSerializable, NewsBean}
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.metrics.sum.Sum
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

object ElasticsearchClient {

  val LOG = LoggerFactory.getLogger(getClass)

  private val host = "localhost"
  private val port = 9300
  private val BULK_THRESHOLD = 500

  val INDEX_NEWS_RC = "news_rc"
  val TYPE_NEWS = "news"
  val TYPE_KEYWORD = "keyword"

  private var isIndexCreated = false

  private var client: Option[Client] = null

  private def getClientInstance(): Client = {
    if(client == null || client.isEmpty) {
      client = getClient(this.host, this.port)
    }
    if(client.isDefined) {
      if(!isIndexCreated) {
        createIndex(client.get)
        isIndexCreated = true
      }
      client.get
    } else {
      throw new RuntimeException("Uable to connect to ElasticsearchClient with host: [" + this.host +"] and port: [" + this.port + "]")
    }
  }

  private def createIndex(client: Client): Unit = {
    val exists = client.admin().indices()
      .prepareExists(INDEX_NEWS_RC)
      .execute().actionGet().isExists()
    if(!exists) {
      client.admin().indices().prepareCreate(INDEX_NEWS_RC)
        .addMapping(TYPE_KEYWORD, "{\n" +
          "    \"keyword\": {\n" +
          "       \"_all\": {\n" +
          "         \"enabled\": false,\n" +
          "         \"omit_norms\": true\n" +
          "       },\n" +
          "       \"properties\": {\n" +
          "         \"newsId\": {\n" +
          "             \"type\": \"string\",\n" +
          "             \"index\": \"not_analyzed\"\n" +
          "         },\n" +
          "         \"keyword\": {\n" +
          "             \"type\": \"string\",\n" +
          "             \"index\": \"not_analyzed\"\n" +
          "         },\n" +
          "         \"score\": {\n" +
          "             \"type\": \"float\"\n" +
          "         }\n" +
          "       }\n" +
          "     }\n" +
          "  }")
        .get()
    }
  }

  private def getClient(hostName: String, port: Int): Option[Client] = {
    getHost(hostName).map(host => new PreBuiltTransportClient(Settings.builder()
      .put("client.transport.sniff",true)
      .put("cluster.name","docker-cluster")
      .put("node.name","elasticsearch1").build())
      .addTransportAddress(new InetSocketTransportAddress(host, port)))
  }

  private def getHost(hostName: String): Option[InetAddress] = {
    var host: InetAddress = null
    try {
      host = InetAddress.getByName(hostName)
    } catch {
      case e: UnknownHostException => {
        LOG.warn("Could not get host: [" + hostName + "]")
      }
    }
    Option(host)
  }

  def queryRelevantNewsIds(keywords: Array[String]): Map[String, Long] = {
    val queryBuilder = keywords.foldLeft(QueryBuilders.boolQuery())((x, y) => {
      x.should(QueryBuilders.termQuery("keyword", y))
    }).minimumShouldMatch(1)
    val sr =  getClientInstance().prepareSearch()
      .setIndices(INDEX_NEWS_RC)
      .setTypes(TYPE_KEYWORD)
      .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
      .setSize(0)
      .setQuery(queryBuilder)
      .addAggregation(AggregationBuilders
        .terms("group_by_newsId")
        .field("newsId")
        .order(Terms.Order.aggregation("sum_by_score", false))
        .subAggregation(
          AggregationBuilders.sum("sum_by_score").field("score")))
      .get()

    val newsIdMap = mutable.Map[String, (Long)]()
    val terms = sr.getAggregations.get[Terms]("group_by_newsId")
    for(entry: Terms.Bucket <- terms.getBuckets) {
      val newsId = entry.getKeyAsString
      val count = entry.getDocCount
      val sum = entry.getAggregations()
        .get[Sum]("sum_by_score").getValue()
      newsIdMap.put(newsId, count)
    }
    newsIdMap.toMap
  }

  def queryNews(newsIds: Array[String]): List[NewsBean] = {
    val queryBuilder = newsIds.foldLeft(QueryBuilders.boolQuery())((x, y) => {
      x.should(QueryBuilders.termQuery("id", y))
    }).minimumShouldMatch(1)
    val sr =  getClientInstance().prepareSearch()
      .setIndices(INDEX_NEWS_RC)
      .setTypes(TYPE_NEWS)
      .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
      .setQuery(queryBuilder)
      .get()

    val hits = sr.getHits()
    newsIds.flatMap(x => {
      val hit = hits.find(y => y.getId.equals(x))
      if(hit.isDefined) {
        val s = hit.get.getSource
        Option(new NewsBean(hit.get.getId, s.get("title").toString, s.get("content").toString))
      } else {
        None
      }
    }).toList
  }

  def bulkLoad(index: String, `type`: String, list: List[EsElement]): Boolean =  {
    var count = 0
    val requestBuilderList = new mutable.ListBuffer[IndexRequestBuilder]()
    while(count < list.size) {
      requestBuilderList += parseToRequestBuilder(index, `type`, list(count))
      if(count > 0 && count % BULK_THRESHOLD == 0) {
        createOrUpdateDocument(count, index, `type`, requestBuilderList.toList)
        requestBuilderList.clear()
      }
      if(count == list.size - 1 && requestBuilderList.size > 0) {
        createOrUpdateDocument(count, index, `type`, requestBuilderList.toList)
      }
      count += 1
    }
    false
  }

  private def parseToRequestBuilder(index: String, `type`: String, obj: EsElement): IndexRequestBuilder = {
    val builder = parseToXContentBuilder(null, obj, jsonBuilder())
    if(obj.isInstanceOf[EsElementNoId]) {
      this.getClientInstance().prepareIndex(index, `type`)
        .setSource(builder)
    } else {
      this.getClientInstance().prepareIndex(index, `type`, obj.id)
        .setSource(builder)
    }
  }

  private def parseToXContentBuilder(objectName: String, obj: Object, builder: XContentBuilder): XContentBuilder = {
    var tmpBuilder = builder
    if(obj.isInstanceOf[Iterable[_]]) {
      val list = obj.asInstanceOf[Iterable[_]]
      if(objectName == null || objectName.length == 0) {
        tmpBuilder = tmpBuilder.startArray()
      } else {
        tmpBuilder = tmpBuilder.startArray(objectName)
      }
      list.foreach(x => {
        tmpBuilder = tmpBuilder.startObject()
        x.getClass().getDeclaredFields
          .foreach(field => {
            field.setAccessible(true)
            if(x.isInstanceOf[EsElementNoId] && field.getName.equals("id")) {
              // ignore
            } else if(isExtractingValueDirectly(field.get(x))) {
              tmpBuilder = tmpBuilder.field(field.getName, field.get(x))
            } else {
              tmpBuilder = parseToXContentBuilder(field.getName, field.get(x), tmpBuilder)
            }
          })
        tmpBuilder = tmpBuilder.endObject()
      })
      tmpBuilder = tmpBuilder.endArray()
    } else if(obj.isInstanceOf[Array[_]]) {
      val list = obj.asInstanceOf[Iterable[_]]
      if(objectName == null || objectName.length == 0) {
        tmpBuilder = tmpBuilder.startArray()
      } else {
        tmpBuilder = tmpBuilder.startArray(objectName)
      }
      list.foreach(x => {
        x.getClass().getDeclaredFields
          .foreach(field => {
            field.setAccessible(true)
            if(x.isInstanceOf[EsElementNoId] && field.getName.equals("id")) {
              // ignore
            } else if(isExtractingValueDirectly(field.get(x))) {
              tmpBuilder = tmpBuilder.field(field.getName, field.get(x))
            } else {
              tmpBuilder = parseToXContentBuilder(field.getName, field.get(x), tmpBuilder)
            }
          })
      })
      tmpBuilder = tmpBuilder.endArray()
    } else if(obj.isInstanceOf[EsSerializable]) {
      if(objectName == null || objectName.length == 0) {
        tmpBuilder = tmpBuilder.startObject()
      } else {
        tmpBuilder = tmpBuilder.startObject(objectName)
      }
      obj.getClass().getDeclaredFields
        .foreach(field => {
          field.setAccessible(true)
          if(obj.isInstanceOf[EsElementNoId] && field.getName.equals("id")) {
            // ignore
          } else if(isExtractingValueDirectly(field.get(obj))) {
            tmpBuilder = tmpBuilder.field(field.getName, field.get(obj))
          } else {
            tmpBuilder = parseToXContentBuilder(field.getName, field.get(obj), tmpBuilder)
          }
        })
      tmpBuilder = tmpBuilder.endObject()
    }
    tmpBuilder
  }

  private def isExtractingValueDirectly(obj: Any): Boolean = {
    var flag = true
    try {
      obj.getClass.getTypeName match {
        case "java.lang.String" | "java.lang.Boolean" |
             "java.lang.Character" | "java.lang.Byte" |
             "java.lang.Short" | "java.lang.Integer" |
             "java.lang.Long" | "java.lang.Float" |
             "java.lang.Double" | "java.lang.Void" => flag = true
        case _ => flag = false
      }
    } catch {
      case e: NullPointerException => {
        flag = true
      }
    }
    flag
  }

  private def createOrUpdateDocument(count: Int, index: String, `type`: String, list: List[IndexRequestBuilder]): Unit = {
    val startCount = if(count - BULK_THRESHOLD > 0) count - BULK_THRESHOLD else 0
    val endCount = startCount + list.size - 1
    LOG.info(s"Starting to import [${list.size}] data from index [${startCount}] to [${endCount}]")
    val bulkRequest = this.getClientInstance().prepareBulk()
    list.foreach(bulkRequest.add)
    val bulkResponse = bulkRequest.get()
    if (bulkResponse.hasFailures()) {
      LOG.error(s"import data from index [${startCount}] to [${endCount}] has failures: " + bulkResponse.buildFailureMessage())
    } else {
      LOG.info(s"[${list.size}] Records are imported")
    }
  }

  def main(args: Array[String]): Unit = {
    println(queryRelevantNewsIds(Array[String]("國軍", "家")))
  }

}
