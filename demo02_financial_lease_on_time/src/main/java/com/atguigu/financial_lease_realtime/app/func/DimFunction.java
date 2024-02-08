package com.atguigu.financial_lease_realtime.app.func;

import com.alibaba.fastjson.JSONObject;

public interface DimFunction<T> {
    String getTable();

    String getId(T bean);

    void addDim(T bean, JSONObject dim);
}
