package com.gaogzhen.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.gaogzhen.gmall.realtime.common.bean.TradeSkuOrderBean;

/**
 * @author yhm
 * @create 2024-01-03 14:15
 */
public interface DimJoinFunction<T> {
    public  String getId( T input);
    public  String getTableName();
    public  void join(T input, JSONObject dim);
}
