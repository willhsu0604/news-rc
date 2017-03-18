package idv.will

import idv.will.annotation.HTableProfile
import idv.will.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap


object HBaseClient {

  val LOG = LoggerFactory.getLogger(getClass)
  private var configured = false
  private var tablePool: HashMap[String, Table] = HashMap[String, Table]()

  var conf: Configuration = null

  @HTableProfile(partitionsPrefix = "0123456789")
  val TABLE_NEWS = "news"

  @HTableProfile(partitionsPrefix = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
  val TABLE_WORD_BASE = "word_base"

  val DEFAULT_FAMILY = Bytes.toBytes("f")

  val QUALIFIER_WORD_BASE = Bytes.toBytes("wb")
  val QUALIFIER_NEWS_TAG = Bytes.toBytes("nt")

  def getConfiguration(): Configuration = {
    val conf:Configuration = HBaseConfiguration.create()
    conf.set("hbase.tmp.dir", "file:///tmp/news-rc-local/hbase")
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.cluster.distributed", "false")
    conf
  }

  def setup(): Unit = {
    this.synchronized {
      if (!configured) {
        conf = getConfiguration()
        val admin = getHBaseConnection.getAdmin
        HBaseClient.getClass.getDeclaredFields.foreach(field => {
          val tableProfile = field.getDeclaredAnnotation(classOf[HTableProfile])
          if (tableProfile != null) {
            field.setAccessible(true)
            val tableNameStr = field.get(this).toString
            val tableName = TableName.valueOf(tableNameStr)
            if (!admin.tableExists(tableName)) {
              val partitioned: Boolean = getTablePartitionPrefix(tableNameStr).length>0
              try {
                val tableDesc: HTableDescriptor = new HTableDescriptor(tableName)
                val compressionType = Compression.Algorithm.GZ
                val family = new HColumnDescriptor(DEFAULT_FAMILY)
                family.setMaxVersions(1)
                family.setMinVersions(1)
                family.setCompressionType(compressionType)
                family.setCompactionCompressionType(compressionType)
                tableDesc.addFamily(family)
                if (partitioned) {
                  admin.createTable(tableDesc, getTablePartitionPrefix(tableNameStr).toCharArray.map(x=>Bytes.toBytes(x+"")))
                } else {
                  admin.createTable(tableDesc)
                }
                LOG.info("Created table '" + tableNameStr + "', partitioned: " + partitioned + ", compression: " + compressionType.getName)
              } catch {
                case e: Exception =>
                  LOG.error("Unable to create table '"+tableNameStr+"'",e)
                  System.exit(-1)
              }
            }
            val table = getHBaseConnection.getTable(tableName)
            table.setWriteBufferSize(1024 * 1024)
            tablePool += (tableNameStr -> table)
          }
        })
      }
      LOG.info("HTable verification completed. " + tablePool.size + " tables verified => " + tablePool.keySet)
      configured = true
    }
  }

  def getPartitionedRowKey(tableName:String,rowKey: String): String = {
    val partitionPrefix=getTablePartitionPrefix(tableName)
    if(partitionPrefix.isEmpty){
      rowKey
    }else{
      partitionPrefix.charAt((Math.abs(rowKey.hashCode) % partitionPrefix.length)) + rowKey
    }
  }

  def getValue[T](table: String, row: String, family: Array[Byte], qualifier: Array[Byte])(implicit m: Manifest[T]) = {
    val get = new Get(Bytes.toBytes(getPartitionedRowKey(table,row)))
    get.addColumn(family, qualifier)
    val result = getHTable(table).get(get)
    val resultValue = result.getValue(family, qualifier)
    JsonUtils.fromJson[T](Bytes.toString(resultValue))
  }

  def bulkLoad(path: String, tableName: String) {
    val inputPath = new Path(path)
    val loader: LoadIncrementalHFiles = new LoadIncrementalHFiles(getHBaseConnection.getConfiguration)
    loader.doBulkLoad(inputPath, new HTable(conf, tableName))
    FileSystem.get(conf).delete(inputPath, true)
  }

  def getHTable(tableName: String): Table = {
    try {
      tablePool.get(tableName).get
    } catch {
      case e: Exception => null
    }
  }

  def getTableInfo(tableName: String) = {
    getHTable(tableName).close()
    val admin = getHBaseConnection.getAdmin
    val regions: List[HRegionInfo] = admin.getTableRegions(TableName.valueOf(tableName)).toList
    regions.map(x => (x.getStartKey, x.getEndKey))
  }

  def writeToHBase(tableName: String, family: Array[Byte], qualifier: Array[Byte], data: Map[String, Any], fullyUpdate:Boolean): Unit = {
    val table: Table = HBaseClient.getHTable(tableName)
    data.par.foreach(data => {
      val (rowKey, value) = (data._1, data._2)
      val put: Put = new Put(Bytes.toBytes(HBaseClient.getPartitionedRowKey(tableName, rowKey)))
      val stringVal = JsonUtils.toJson(value)
      put.addColumn(family, qualifier, Bytes.toBytes(stringVal))
      table.put(put)
    })
    if(fullyUpdate){
      //TODO: implement fully update by removing old entries
      LOG.warn("Fully update is not yet implemented")
    }
    LOG.info("Loaded local data set into table:'" + tableName + "', family:'" + "'" + Bytes.toString(family) + "', qualifier:'" + Bytes.toString(qualifier) + "'")

  }

  val partitionTableCache: HashMap[String, String] = new HashMap[String, String]()
  def getTablePartitionPrefix(tableName: String): String = {
    val result = partitionTableCache.get(tableName)
    if (result.isDefined) {
      return result.get
    }
    tableName.synchronized{
      HBaseClient.getClass.getDeclaredFields.foreach(field => {
        val tableProfile = field.getDeclaredAnnotation(classOf[HTableProfile])
        if (tableProfile != null) {
          field.setAccessible(true)
          val partitionsPrefix = field.getAnnotation(classOf[HTableProfile]).partitionsPrefix()
          if (field.get(this).toString.equals(tableName)) {
            partitionTableCache += ((tableName, partitionsPrefix))
            return partitionsPrefix
          }
        }
      })
    }
    return null
  }


  private var hbaseConnection: Connection = null
  def getHBaseConnection = {
    getClass.synchronized {
      if (hbaseConnection == null || hbaseConnection.isClosed) {
        hbaseConnection = ConnectionFactory.createConnection(conf)
      }
    }
    hbaseConnection
  }

}
