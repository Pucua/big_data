package com.atguigu.financial_lease_realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.financial_lease_realtime.common.FinancialLeaseCommon;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class HBaseUtil {
    /**
     * 创建HBase连接
     * @return
     */
    public static Connection getHBaseConnection(){
        Configuration conf = HBaseConfiguration.create();
        conf.set(FinancialLeaseCommon.HBASE_ZOOKEEPER_QUORUM,FinancialLeaseCommon.HBASE_ZOOKEEPER_QUORUM_HOST);
        conf.set(FinancialLeaseCommon.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT,FinancialLeaseCommon.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT_VALUE);

        try {
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 关闭HBase连接
     * @param connection
     */
    public static void closeHBaseConnection(Connection connection){
        if(connection != null && !connection.isClosed()){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void createTable(Connection hBaseConnection, String hbaseNamespace, String tableName, String... familyNames) throws IOException {
        // 判断列族是否存在
        if(familyNames == null || familyNames.length==0){
            throw new RuntimeException("列族至少有一个");
        }
        //  hbase的api分两类：DDL,DML
        // DDL使用admin
        // DML使用table
        Admin admin = hBaseConnection.getAdmin();
        TableName tableName1 = TableName.valueOf(hbaseNamespace, tableName);

        try{
            // 判断表格是否已存在
            if(admin.tableExists(tableName1)){
                System.out.println("想要创建的表格" + hbaseNamespace + ":" + tableName + "已经存在了");
            }else{
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName1);
                for (String familyName : familyNames) {
                    ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName));
                    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
                }

                admin.createTable(tableDescriptorBuilder.build());
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        admin.close();
    }


    public  static void deleteTable(Connection hBaseConnection, String hbaseNamespace, String tableName) throws IOException {
        Admin admin = hBaseConnection.getAdmin();
        TableName tableName1 = TableName.valueOf(hbaseNamespace, tableName);

        try{
            // 删除表格之前先标记disable
            admin.disableTable(tableName1);
            admin.deleteTable(tableName1);
        }catch (IOException e){
            e.printStackTrace();
        }

        admin.close();
    }

    public static void deleteRow(Connection hBaseConnection, String hbaseNamespace, String sinkTable, String rowKeyValue) throws IOException {
        Table table = hBaseConnection.getTable(TableName.valueOf(hbaseNamespace, sinkTable));
        try {
            table.delete(new Delete(Bytes.toBytes(rowKeyValue)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
    }

    public static void putRow(Connection hBaseConnection, String hbaseNamespace, String sinkTable, String rowKeyValue, String sinkFamily, String[] columns, String[] values) throws IOException {
        Table table = hBaseConnection.getTable(TableName.valueOf(hbaseNamespace, sinkTable));
        Put put = new Put(Bytes.toBytes(rowKeyValue));
        for (int i = 0; i < columns.length; i++) {
            if(values[i] == null){   // 表格空的转为空字符串，防止指针异常
                values[i] = "";
            }
            put.addColumn(Bytes.toBytes(sinkFamily),Bytes.toBytes(columns[i]),Bytes.toBytes(values[i]));
        }

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        table.close();
    }

    public static AsyncConnection getHBaseAsyncConnection() throws ExecutionException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set(FinancialLeaseCommon.HBASE_ZOOKEEPER_QUORUM,FinancialLeaseCommon.HBASE_ZOOKEEPER_QUORUM_HOST);
        conf.set(FinancialLeaseCommon.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT,FinancialLeaseCommon.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT_VALUE);

        return ConnectionFactory.createAsyncConnection(conf).get();
    }

    public static void closeHBaseAsyncConnection(AsyncConnection conn){
        if(conn != null && conn.isClosed()){
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 异步读取HBase中的维度数据
     * @param hBaseAsyncConnection
     * @param hbaseNamespace
     * @param tableName
     * @param rowKey
     */
    public static JSONObject asyncReadDim(AsyncConnection hBaseAsyncConnection, String hbaseNamespace, String tableName, String rowKey) {
        AsyncTable<AdvancedScanResultConsumer> table = hBaseAsyncConnection.getTable(TableName.valueOf(hbaseNamespace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        CompletableFuture<Result> resultFuture = table.get(get);
        JSONObject dim = new JSONObject();

        try{
            Result result = resultFuture.get();
            for (Cell cell : result.rawCells()) {
                String column = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell));
                dim.put(column,value);
            }
            return dim;
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("读取维度数据异常");
        }

        return  dim;


    }



}
