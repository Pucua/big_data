package com.atguigu.gmall.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;

public class ExplodeJSONArray extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        // 1 参数合法性检查
        if(argOIs.length != 1){
            throw new UDFArgumentException("explode_json_array函数只能接收一个参数");
        }

        // 2 第一个参数必须为string
        //判断参数是否为基础数据类型
        ObjectInspector argOI = argOIs[0];
        if(argOI.getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentException("explode_json_array函数只能接收基本数据类型的参数");
        }

        //将参数对象检查器强转为基础类型对象检查器
        PrimitiveObjectInspector primitiveOI = (PrimitiveObjectInspector) argOI;
        if(primitiveOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING){
            throw new UDFArgumentException("explode_json_array函数只能接收STRING数据类型的参数");
        }

        // 3 定义返回值名称和类型
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("item");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }


    public void process(Object[] objects) throws HiveException {

        // 1 获取传入的数据
        String jsonArray = objects[0].toString();

        // 2 将string转换为json数组
        JSONArray actions = new JSONArray(jsonArray);

        // 3 循环一次，取出数组中的一个json，并写出
        for (int i = 0; i < actions.length(); i++) {

            String[] result = new String[1];
            result[0] = actions.getString(i);
            forward(result);
        }
    }


    public void close() throws HiveException {

    }
}
