package cn.itcast.kafka.util

import java.sql.{DriverManager, ResultSet}

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

object OffsetUtil {
  /**
    * 从数据库读取偏移量
    */
  def getOffsetMap(groupid: String, topic: String) = {
    val connection = DriverManager.getConnection("jdbc:mysql://node01:3306/bigdata?characterEncoding=UTF-8",
      "root", "123456")
    val pstmt = connection.prepareStatement("select * from t_offset where groupid=? and topic=?")
    pstmt.setString(1, groupid)
    pstmt.setString(2, topic)
    val rs: ResultSet = pstmt.executeQuery()
    val offsetMap = mutable.Map[TopicPartition, Long]()
    while (rs.next()) {
      offsetMap += new TopicPartition(rs.getString("topic")
        , rs.getInt("partition")) -> rs.getLong("offset")
    }
    rs.close()
    pstmt.close()
    connection.close()
    offsetMap
  }

  /**
    * 将偏移量保存到数据库
    */
  def saveOffsetRanges(groupid: String, offsetRange: Array[OffsetRange]) = {
    val connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8"
      , "root", "123456")
    //replace into表示之前有就替换,没有就插入
    val pstmt = connection.prepareStatement("replace into t_offset (`topic`, `partition`, `groupid`, `offset`) values(?,?,?,?)")
    for (o <- offsetRange) {
      pstmt.setString(1, o.topic)
      pstmt.setInt(2, o.partition)
      pstmt.setString(3, groupid)
      pstmt.setLong(4, o.untilOffset)
      pstmt.executeUpdate()
    }
    pstmt.close()
    connection.close()
  }
}
