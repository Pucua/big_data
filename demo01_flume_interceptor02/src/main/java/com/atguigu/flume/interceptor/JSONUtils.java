package com.atguigu.flume.interceptor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;

public class JSONUtils {
//    public static void main(String[] args) {
//        System.out.println(isJSONValidate("{111"));       false
//        System.out.println(isJSONValidate("{\"age\":10}"));  true
//    }

    public static boolean isJSONValidate(String log){
        try {
            JSON.parse(log);
            return true;
        }catch (JSONException e){
            return false;
        }
    }

}
