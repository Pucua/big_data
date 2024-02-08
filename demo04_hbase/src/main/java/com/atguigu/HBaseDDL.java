package com.atguigu;

import com.sun.org.apache.bcel.internal.generic.RET;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseDDL {
    // 声明一个静态属性
    public static Connection connection = HBaseConnection.connection;

    /**
     * 创建命名空间
     * @param namespace  命名空间名
     */
    public static void createNameSpace(String namespace) throws IOException {
        // 1.获取admin
        // admin的连接为轻量级，非线程安全，不推荐池化或缓存连接
        Admin admin = connection.getAdmin();

        // 2.调用方法创建命名空间
        // 代码相对shell更底层，shell能实现的功能代码能实现

        // 2.1创建命名空间描述构建者--设计师
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);

        // 2.2给命名空间添加需求
        builder.addConfiguration("user","atguigu");

        // 2.3使用builder构建对应的添加完参数的对象 完成创建
        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            System.out.println("该命名空间已经存在");
            e.printStackTrace();
        }

        // 3.关闭admin
        admin.close();
    }

    /**
     * 判断表格是否存在
     * @param namespace
     * @param tableName
     * @return true表示存在
     */
    public static boolean isTableExists(String namespace,String tableName) throws IOException {
        // 获取admin
        Admin admin = connection.getAdmin();
        // 判断表格是否存在
        boolean b = false;
        try {
            b = admin.tableExists(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        // 关闭admin
        admin.close();
        // 返回结果
        return b;

    }

    /**
     * 创建表格
     * @param namespace
     * @param tableName
     * @param columnFamilies 列族名，可多个
     */
    public static void createTable(String namespace,String tableName, String... columnFamilies) throws IOException {
        // 0.1判断是否有一个列族
        if(columnFamilies.length == 0){
            System.out.println("创建表格至少有一个列族");
            return;
        }

        // 0.2判断表格是否存在
        if(isTableExists(namespace,tableName)){
            System.out.println("表格已经存在");
            return;
        }

        // 1.获取admin
        Admin admin = connection.getAdmin();

        // 2.调用方法创建表格
        // 2.1创建表格描述的建造者
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));

        // 2.2添加参数
        for (String columnFamily : columnFamilies) {
            // 2.3创建列族描述的建造者
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
            // 2.4对应当前的列族添加参数
            // 版本参数
            columnFamilyDescriptorBuilder.setMaxVersions(5);
            // 2.5 创建添加完参数的列族描述
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }

        // 2.6创建对应的表格描述
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println("表格已经存在");
            e.printStackTrace();
        }

        // 3.关闭admin
        admin.close();

    }

    /**
     * 修改表格中一个列族的版本
     * @param namespace
     * @param tableName
     * @param columnFamily
     * @param version
     */
    public static void modifyTable(String namespace,String tableName, String columnFamily,int version) throws IOException {
        // 判断表格是否存在
        if(!isTableExists(namespace,tableName)){
            System.out.println("表格不存在");
            return;
        }

        // 1.获取admin
        Admin admin = connection.getAdmin();

        try {
            // 2.调用方法修改表格
            // 2.0获取之前的表格描述
            TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(namespace, tableName));

            // 2.1创建一个表格描述建造者
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(descriptor);

            // 2.2对应建造者进行表格数据的修改
            ColumnFamilyDescriptor columnFamily1 = descriptor.getColumnFamily(Bytes.toBytes(columnFamily));
            // 创建列族描述建造者
            // 需要填写旧的列族描述
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily1);

            // 修改对应的版本
            columnFamilyDescriptorBuilder.setMaxVersions(version);

            // 修改填写新创建或参数初始化
            tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());
            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3.关闭admin
        admin.close();
    }

    /**
     *
     * @param namespace
     * @param tableName
     */
    public static boolean deleteTable(String namespace,String tableName) throws IOException {
        // 1.判断表格是否存在
        if(!isTableExists(namespace,tableName)){
            System.out.println("表格不存在无法删除");
            return false;
        }

        // 2.获取admin
        Admin admin = connection.getAdmin();

        // 3.调用相关方法删除表格
        try {
            // 先disable后delete
            TableName tableName1 = TableName.valueOf(namespace, tableName);
            admin.disableTable(tableName1);
            admin.deleteTable(tableName1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 4.删除admin
        admin.close();

        return true;
    }

    public static void main(String[] args) throws IOException {
        // 测试创建命名空间
        createNameSpace("atguigu");

        // 测试表格是否存在
        isTableExists("bigdata","student");

        // 测试创建表格
        createTable("atguigu","student","info","msg");

        // 测试修改表格
        modifyTable("atguigu","student","info1",6);

        // 测试删除表格
        deleteTable("atguigu","student");

        System.out.println("其他代码");

        // 关闭HBase的连接
        HBaseConnection.closeConnection();
    }
}
