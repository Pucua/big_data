package com.atguigu.financial_lease_realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.financial_lease_realtime.bean.TransientSink;

import java.lang.reflect.Field;

// 将javaBean转为json字符串
public class Bean2JSONUtil {
    public static <T> String bean2JSON(T obj) throws IllegalAccessException {
        JSONObject jsonObject = new JSONObject();

        // 使用反射获取类模板
        Class<?> clazz = obj.getClass();
        // 获取属性名称
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            TransientSink annotation = field.getAnnotation(TransientSink.class);
            if(annotation == null){
                // 只有不标注注解的属性才需要转换为字符串
                // 需要开启字段的读取权限
                field.setAccessible(true);
                Object value = field.get(obj);
                String name = field.getName();

                // 将属性名称和对应的属性值写到jsonObj对象
                StringBuilder snakeCaseName = new StringBuilder();
                for (int i = 0; i < name.length(); i++) {
                    char c = name.charAt(i);
                    if(Character.isUpperCase(c)){
                        snakeCaseName.append("_");
                        c = Character.toLowerCase(c);
                    }
                    snakeCaseName.append(c);
                }
                jsonObject.put(snakeCaseName.toString(),value);
            }

        }

        return jsonObject.toJSONString();

    }
}
