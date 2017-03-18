package idv.will

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}

object HBaseTest {

  def main(args:Array[String]):Unit={
    val conf:Configuration=HBaseConfiguration.create()
    val connection:Connection = ConnectionFactory.createConnection(conf)
    val admin:Admin=connection.getAdmin
    val cf:HColumnDescriptor=new HColumnDescriptor("nr")
    val tableName:TableName=TableName.valueOf("test")
    val table:HTableDescriptor=new HTableDescriptor(tableName)
    table.addFamily(cf)
    admin.createTable(table)
  }
}
