package com.atguigu.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class ETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 取数据后进行校验

        // 1.获取数据
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);

        // 2.校验
        if(JSONUtils.isJSONValidate(log)){
            return event;
        }else{
            return null;
        }


    }

    @Override
    public List<Event> intercept(List<Event> list) {
//        Iterator<Event> iterator = list.iterator();
//
//        while (iterator.hasNext()){
//            Event next = iterator.next();
//            if(intercept(next)==null){
//                iterator.remove();
//            }
//        }

        list.removeIf(next -> intercept(next) == null);

        return list;
    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }

    @Override
    public void close() {

    }

}
