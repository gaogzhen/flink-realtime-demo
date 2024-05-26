package com.gaogzhen.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @author yhm
 * @create 2023-12-27 15:33
 */
public class  DorisMapFunction<T> implements MapFunction<T, String> {
    @Override
    public String map(T value) throws Exception {
        SerializeConfig config = new SerializeConfig();
        config.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
        return  JSONObject.toJSONString(value, config);
    }
}
