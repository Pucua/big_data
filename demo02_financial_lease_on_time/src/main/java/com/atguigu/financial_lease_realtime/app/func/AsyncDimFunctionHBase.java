package com.atguigu.financial_lease_realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.financial_lease_realtime.bean.DwsAuditIndLeaseOrgSalesmanApprovalBean;
import com.atguigu.financial_lease_realtime.common.FinancialLeaseCommon;
import com.atguigu.financial_lease_realtime.util.HBaseUtil;
import com.atguigu.financial_lease_realtime.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AsyncDimFunctionHBase<T> extends RichAsyncFunction<T, T> implements DimFunction<T>{

    AsyncConnection hBaseAsyncConnection = null;
    StatefulRedisConnection<String, String> asyncRedisConnection;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取hbase的异步连接
        hBaseAsyncConnection = HBaseUtil.getHBaseAsyncConnection();
        // 获取redis的异步连接
        asyncRedisConnection = RedisUtil.getAsyncRedisConnection();
    }

    @Override
    public void close() throws Exception {
        // 关闭hbase的异步连接
        HBaseUtil.closeHBaseAsyncConnection(hBaseAsyncConnection);

        asyncRedisConnection.close();

    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        // 关联数据，将合并之后的数据写出
        CompletableFuture
                .supplyAsync(new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        // 1.优先从redis中读取维度数据
                        return RedisUtil.asyncReadDim(asyncRedisConnection,getTable() + ":" + getId(input));
                    }
                })
                .thenApplyAsync(new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject jsonObject) {
                        // 2.如果 redis中没有需要的维度数据，再从HBase中读取维度数据
                        if(jsonObject == null){
                            System.out.println("redis中没有需要读取的数据，要从hbase中读取" + getTable() + ":" + getId(input));
                            String table = getTable();
                            String id = getId(input);
                            JSONObject dim = HBaseUtil.asyncReadDim(hBaseAsyncConnection, FinancialLeaseCommon.HBASE_NAMESPACE, table, id);

                           // 3.要将数据写到redis中
                            RedisUtil.asyncWriteDim(asyncRedisConnection,getTable()+":"+getId(input),dim);
                            return dim;
                        }else{
                            // 当前维度数据不为空，说明redis中已经有了维度数据
                            System.out.println("redis中存在对应的维度数据，直接返回" + getTable() + ":" + getId(input));
                            return jsonObject;
                        }
                    }
                })
                .thenAccept(new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject jsonObject) {
                        addDim(input,jsonObject);
                        // 将结果写出
                        resultFuture.complete(Collections.singleton(input));
                    }
                });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        // 如果超时无法读取到维度数据
        System.out.println("异步读取dim维度数据超时");
    }
}
