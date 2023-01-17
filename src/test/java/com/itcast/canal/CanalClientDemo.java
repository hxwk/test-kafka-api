package com.itcast.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Author itcast
 * Date 2020/3/5 16:56
 * Desc 这个类主要是canal客户端连接canal服务器端的demo测试
 * 这部分功能尽量自己实现
 * 主要包括4个步骤：
 * 1. 创建连接
 * 2. 建立连接
 * 3. 获取连接
 * 4. 关闭连接
 */
public class CanalClientDemo {
    public static void main(String[] args) {
        //1.创建连接
        CanalConnector canalConnector = CanalConnectors
                .newSingleConnector(new InetSocketAddress("192.168.139.100", 11111),
                        "itcast_shop",
                        "canal",
                        "canal");

        //2.建立连接
        //指定一次获取数据条数
        int batchSize = 1000;
        boolean isRunning = true;
        try {
            while (isRunning) {
                //建立连接
                canalConnector.connect();
                //回滚上一次的get请求，重新获取数据
                canalConnector.rollback();

                //订阅匹配日志
                canalConnector.subscribe("itcast_shop.*");
                while (isRunning) {
                    //批量拉取binlog日志，一次拉取多条数据
                    Message message = canalConnector.getWithoutAck(batchSize);
                    //获取batchid
                    long batchid = message.getId();
                    //获取binlog数据的条数
                    int size = message.getEntries().size();
                    if (size == 0 || size == -1) {
                        //表示没有获取到数据
                    } else {
                        //有数据，打印数据
                        printSummary(message);
                    }
                }

            }
        } catch (CanalClientException e) {
            e.printStackTrace();
        } finally {
            //断开连接
            canalConnector.disconnect();
        }
    }

    private static void printSummary(Message message) {
        // 遍历整个batch中的每个binlog实体
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 事务开始
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            // 获取binlog文件名
            String logfileName = entry.getHeader().getLogfileName();
            // 获取logfile的偏移量
            long logfileOffset = entry.getHeader().getLogfileOffset();
            // 获取sql语句执行时间戳
            long executeTime = entry.getHeader().getExecuteTime();
            // 获取数据库名
            String schemaName = entry.getHeader().getSchemaName();
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取事件类型 insert/update/delete
            String eventTypeName = entry.getHeader().getEventType().toString().toLowerCase();

            System.out.println("logfileName" + ":" + logfileName);
            System.out.println("logfileOffset" + ":" + logfileOffset);
            System.out.println("executeTime" + ":" + executeTime);
            System.out.println("schemaName" + ":" + schemaName);
            System.out.println("tableName" + ":" + tableName);
            System.out.println("eventTypeName" + ":" + eventTypeName);

            CanalEntry.RowChange rowChange = null;

            try {
                // 获取存储数据，并将二进制字节数据解析为RowChange实体
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }

            // 迭代每一条变更数据
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                // 判断是否为删除事件
                if (entry.getHeader().getEventType() == CanalEntry.EventType.DELETE) {
                    System.out.println("---delete---");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("---");
                }
                // 判断是否为更新事件
                else if (entry.getHeader().getEventType() == CanalEntry.EventType.UPDATE) {
                    System.out.println("---update---");
                    printColumnList(rowData.getBeforeColumnsList());
                    System.out.println("---");
                    printColumnList(rowData.getAfterColumnsList());
                }
                // 判断是否为插入事件
                else if (entry.getHeader().getEventType() == CanalEntry.EventType.INSERT) {
                    System.out.println("---insert---");
                    printColumnList(rowData.getAfterColumnsList());
                    System.out.println("---");
                }
            }
        }
    }

    // 打印所有列名和列值
    private static void printColumnList(List<CanalEntry.Column> columnList) {
        for (CanalEntry.Column column : columnList) {
            System.out.println(column.getName() + "\t" + column.getValue());
        }
    }


}
