package com.atguigu.financial_lease_realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.financial_lease_realtime.bean.TableProcess;
import com.atguigu.financial_lease_realtime.common.FinancialLeaseCommon;
import com.atguigu.financial_lease_realtime.util.HBaseUtil;
import com.atguigu.financial_lease_realtime.util.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import redis.clients.jedis.Jedis;

public class DimSinkFunc  extends RichSinkFunction<Tuple3<String, JSONObject, TableProcess>> {

    private Connection hBaseConnection = null;
    private Jedis jedis = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取hbase的连接
        hBaseConnection = HBaseUtil.getHBaseConnection();

        jedis = RedisUtil.getRedisClient();
    }

    @Override
    public void invoke(Tuple3<String, JSONObject, TableProcess> value, Context context) throws Exception {
        // 拆分value:已经去掉了bootstrap-start,bootstrap-complete
        String type = value.f0;
        JSONObject data = value.f1;
        TableProcess tableProcess = value.f2;

        // 写到hbase的表格
        String sinkTable = tableProcess.getSinkTable();
        String sinkFamily = tableProcess.getSinkFamily();
        // 写到HBase的主键
        String sinkRowKeyName = tableProcess.getSinkRowKey();
        String rowKeyValue = data.getString(sinkRowKeyName);

        String[] columns = tableProcess.getSinkColumns().split(",");
        String[] values = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            values[i] = data.getString(columns[i]);
        }

        if("delete".equals(type)){
            // 删除对应的维度数据
            HBaseUtil.deleteRow(hBaseConnection, FinancialLeaseCommon.HBASE_NAMESPACE,sinkTable,rowKeyValue);
        }else{
            HBaseUtil.putRow(hBaseConnection, FinancialLeaseCommon.HBASE_NAMESPACE,sinkTable,rowKeyValue,sinkFamily,columns,values);
        }

        if("delete".equals(type) || "update".equals(type)){
            // 当维度数据发生变化的时候，需要同步将redis对应的维度数据删除
            jedis.del(sinkTable + ":" + rowKeyValue);
        }
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConnection(hBaseConnection);
    }
}
