package idv.will

import java.util.Properties

import org.apache.hadoop.hbase.LocalHBaseCluster
import org.apache.zookeeper.server.{ServerConfig, ZooKeeperServerMain}
import org.apache.zookeeper.server.quorum.QuorumPeerConfig
import org.slf4j.LoggerFactory

object HBaseServer {

  val LOG = LoggerFactory.getLogger(getClass)
  val clientPort="2181"
  val dataDir="/tmp/news-rc-local/zkdata"
  var server:ZooKeeperServerMain=new ZooKeeperServerMain

  def main(args: Array[String]): Unit = {
    Runtime.getRuntime.exec("rm -Rf /news-rc-local")
    val quorumConfiguration:QuorumPeerConfig = new QuorumPeerConfig()
    val zkProperties:Properties=new Properties()
    zkProperties.setProperty("clientPort",clientPort)
    zkProperties.setProperty("dataDir",dataDir)
    quorumConfiguration.parseProperties(zkProperties)
    val serverConfig:ServerConfig = new ServerConfig()
    serverConfig.readFrom(quorumConfiguration)
    LOG.info("Start running ZK on port "+clientPort)
    new Thread(new Runnable(){
      override def run(): Unit = {
        server.runFromConfig(serverConfig)
      }
    }).start()


    val conf = HBaseClient.getConfiguration()
    val cluster:LocalHBaseCluster=new LocalHBaseCluster(conf,1,1)
    cluster.startup()
    LOG.info("HBase server started")
  }
}
