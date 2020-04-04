package com.haozz.canal_client_demo.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author: haozz
 * @date: 2020/4/4 23:46
 */
public class CanalClient {

    /**
     * canal_server地址，这里是我本地的虚拟机
     */
    private static String SERVER_ADDRESS = "192.168.124.26";

    private static Integer PORT = 11111;

    /**
     * 目的地
     * canal server中的数据实际上是放在内部的消息队列中，这里需要指定队列的名称
     */
    private static String DESTINATION = "example";

    private static String USERNAME = "";

    private static String PASSWORD = "";


    public static void main(String[] args) {
        //建立简介
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(SERVER_ADDRESS, PORT), DESTINATION, USERNAME, PASSWORD);
        canalConnector.connect();

        //订阅所有库所有表下的数据变动
        canalConnector.subscribe(".*\\..*");
        canalConnector.rollback();


        for (; ; ) {

            // 获取指定数量的数据，但不做确认
            // 不做确认的含义是，canal server不做标记，下次取还能取到
            Message message = canalConnector.getWithoutAck(100);
            long messageId = message.getId();
            if (messageId != -1) {
//                System.out.println(message.getEntries());
                System.out.println("messageId -> " + messageId);
                printEntity(message.getEntries());
            }
        }
    }


    public static void printEntity(List<CanalEntry.Entry> entries) {
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                continue;
            }

            try {
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    String tableName = entry.getHeader().getTableName();
//                    System.out.println(rowChange.getEventType());
                    switch (rowChange.getEventType()) {
                        case INSERT:
                            // do something ...
                            System.out.println("this is INSERT for " + tableName + "-> " + rowData.toString());
                            break;
                        case DELETE:
                            // do something ...
                            System.out.println("this is DELETE for " + tableName + "-> " + rowData.toString());
                            break;
                        case UPDATE:
                            // do something ...
                            System.out.println("this is UPDATE for " + tableName + "-> " + rowData.toString());
                            break;
                        default:
                            break;
                    }
                }
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
    }


}
