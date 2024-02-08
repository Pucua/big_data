package com.atguigu;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileFilter;
import java.io.IOException;

public class HBaseDML {
    // 声明一个静态属性
    public static Connection connection = HBaseConnection.connection;

    /**
     * 插入数据
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     * @param value
     */
    public static void putCell(String namespace,String tableName,String rowKey,String columnFamily,String columnName,String value) throws IOException {
        // 1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace,tableName));

        // 2.调用相关方法插入数据
        // 2.1创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        // 2.2给put对象添加数据
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(value));

        // 2.3将对象写入对应的方法
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3.关闭table
        table.close();
    }

    /**
     * 读取数据，读取对应的一行中的某一列
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param columnFamily 列族名
     * @param columnName 列名
     */
    public static void getCells(String namespace,String tableName,String rowKey,String columnFamily,String columnName) throws IOException {
        // 1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace,tableName));

        // 2.创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));

        // 如果直接调用get读取一整行数据
        get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));

        // 设置读取数据的版本
        get.readAllVersions();

        Result result = null;
        try {
            // 读取数据，得到result对象
            result = table.get(get);

            // 处理数据
            Cell[] cells = result.rawCells();

            // 测试方法：把数据打印到控制台，开发需要额外方法对应处理数据
            for (Cell cell : cells) {
                String value = new String(CellUtil.cloneValue(cell));
                System.out.println(value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3.关闭table
        table.close();
    }

    /**
     * 扫描数据
     * @param namespace
     * @param tableName
     * @param startRow  开始row
     * @param stopRow 结束行，不包含
     */
    public static void scanRows(String namespace,String tableName,String startRow, String stopRow) throws IOException {
        // 1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace,tableName));

        // 2.创建scan对象
        Scan scan = new Scan();

        // 添加参数控制扫描的数据
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));

        try {
            // 读取多行数据，获取scanner
            ResultScanner scanner = table.getScanner(scan);

            // result记录一行数据   cell数组
            // ResultScanner记录多行数据  result的数据
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();

                for (Cell cell : cells) {
                    // cloneQualifier列名
                    System.out.println(new String(CellUtil.cloneRow(cell)) + "-"
                            + new String(CellUtil.cloneFamily(cell)) + "-"
                            + new String(CellUtil.cloneQualifier(cell)) + "-"
                            + new String(CellUtil.cloneValue(cell)) + "\t"
                    );
                    System.out.println();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3.关闭table
        table.close();
    }


    /**
     * 带过滤的扫描
     * @param namespace
     * @param tableName
     * @param startRow
     * @param stopRow
     * @param columnFamily
     * @param columnName
     * @param value
     * @throws IOException
     */
    public static void filterScan(String namespace,String tableName,String startRow, String stopRow,String columnFamily,String columnName,String value) throws IOException {
        // 1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace,tableName));

        // 2.创建scan对象
        Scan scan = new Scan();

        // 添加参数控制扫描的数据
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow));

        // 可以添加多个过滤
        FilterList filterList = new FilterList();

        // do 创建过滤器
        // 1) 结果只保留当前列的数据
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                // 列族，列名
                Bytes.toBytes(columnFamily),
                Bytes.toBytes(columnName),
                // 比较关系
                CompareOperator.EQUAL,
                // 值
                Bytes.toBytes(value)
        );
        filterList.addFilter(columnValueFilter);

        // 2) 结果保留整行数据
        // 结果也会保留没有当前列的数据
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                // 列族，列名
                Bytes.toBytes(columnFamily),
                Bytes.toBytes(columnName),
                // 比较关系
                CompareOperator.EQUAL,
                // 值
                Bytes.toBytes(value)
        );
        filterList.addFilter(singleColumnValueFilter);

        // 添加过滤
        scan.setFilter(filterList);


        try {
            // 读取多行数据，获取scanner
            ResultScanner scanner = table.getScanner(scan);

            // result记录一行数据   cell数组
            // ResultScanner记录多行数据  result的数据
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();

                for (Cell cell : cells) {
                    // cloneQualifier列名
                    System.out.println(new String(CellUtil.cloneRow(cell)) + "-"
                            + new String(CellUtil.cloneFamily(cell)) + "-"
                            + new String(CellUtil.cloneQualifier(cell)) + "-"
                            + new String(CellUtil.cloneValue(cell)) + "\t"
                    );
                    System.out.println();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3.关闭table
        table.close();
    }

    /**
     * 删除一行中的一列数据
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param columnName
     */
    public static void deleteColumn(String namespace,String tableName,String rowKey,String columnFamily,String columnName) throws IOException {
        // 1.获取table
        Table table = connection.getTable(TableName.valueOf(namespace,tableName));

        // 2.创建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));

        // do 添加列信息
        // addColumn删除一个版本
        delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        // addColumns删除所有版本：按逻辑删除所有版本数据
        delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));

        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3.关闭table
        table.close();
    }

    public static void main(String[] args) throws IOException {
        // 测试添加数据
        putCell("bigdata","student","2001","info","name","wangwu");

        // 测试读取数据
        getCells("bigdata","student","2001","info","name");

        // 测试扫描数据
        scanRows("bigdata","student","1001","1004");

        // 测试带过滤的单列扫描|整行扫描
        filterScan("bigdata","student","1001","1004","info","name","wangwu");

        // 测试删除数据
        deleteColumn("bigdata","student","2001","info","name");

        // 其他代码：测试是否到此步表明成功
        System.out.println("其他代码");

        // 关闭连接
         HBaseConnection.closeConnection();

    }
}
