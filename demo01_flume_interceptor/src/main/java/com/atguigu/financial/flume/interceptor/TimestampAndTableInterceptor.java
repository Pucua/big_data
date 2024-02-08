package com.atguigu.financial.flume.interceptor;


import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;   // 别导错了

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TimestampAndTableInterceptor implements Interceptor {

    @Override   // 初始化生命周期,获取连接
    public void initialize() {

    }

    @Override   // 处理单个event，解耦
    public Event intercept(Event event) {
//  {"database":"financial_lease","table":"test","type":"insert","ts":1695003339,"xid":15379,"commit":true,"data":{"id":5,"name":"eee"}}

        // 需要event的header中添加业务数据时间错和表格名称
        Map<String, String> headers = event.getHeaders();

        // ,从body中提取json格式数据
        String log = new String(event.getBody(), StandardCharsets.UTF_8);
        try{
            JSONObject jsonObject = JSONObject.parseObject(log);   // 健壮性：获取时可能非json格式返回null
            String tableName = jsonObject.getString("table");
            // maxwell数据中的时间戳是10位，正常一般用13位,ts*1000
            String ts = jsonObject.getString("ts")  + "000";

            // 需要对应的上flume中HDFS sink https://flume.apache.org/releases/content/1.10.0/FlumeUserGuide.html
            headers.put("tableName",tableName);
            headers.put("timestamp",ts);
        }catch (JSONException e){
            return null;
        }
        return event;
    }

    @Override  // 处理多条数据，过滤
    public List<Event> intercept(List<Event> events) {
        // 批量处理event，同时处理过滤功能 alt+enter使用lambda代换
        events.removeIf(next -> intercept(next) == null);

//        Iterator<Event> iterator = events.iterator();
//        while(iterator.hasNext()){
//            Event next = iterator.next();
//            if(intercept(next)==null){
//                iterator.remove();
//            }
//        }
        return events;
    }

    @Override  // 关闭方法
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new TimestampAndTableInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
