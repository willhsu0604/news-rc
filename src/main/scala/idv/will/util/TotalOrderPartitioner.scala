package idv.will.util

import com.google.common.primitives.UnsignedBytes
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.Partitioner

class TotalOrderPartitioner(partitionInfo: List[(Array[Byte], Array[Byte])]) extends Partitioner {

  def numPartitions: Int = partitionInfo.size

  def getPartition(partitionKey: Any): Int = {
    val keyAsString = partitionKey.toString
    if (partitionInfo.size > 1) {
      if (UnsignedBytes.lexicographicalComparator().compare(Bytes.toBytes(keyAsString), partitionInfo(numPartitions - 1)._1) >= 0) {
        return numPartitions - 1
      }
      for (i <- 0 to partitionInfo.size - 1) {
        if (isBetween(partitionInfo(i)._1, Bytes.toBytes(keyAsString), partitionInfo(i)._2)) {
          return i
        }
      }
    }
    return 0
  }

  def isBetween(left: Array[Byte], target: Array[Byte], right: Array[Byte]): Boolean = {
    val compareLeft = UnsignedBytes.lexicographicalComparator().compare(target, left)
    if (compareLeft < 0) false
    else if (compareLeft == 0 || UnsignedBytes.lexicographicalComparator().compare(right, target) > 0) true
    else false
  }

}