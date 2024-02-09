package com.atguigu.financial.lease.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @author yhm
 * @create 2023-08-29 16:26
 */
public interface DimFunction<T> {
    String getTable();

    String getId(T bean);

    void addDim(T bean, JSONObject dim);
}
