package com.gaogzhen.gmall.realtime.dim.app;

import com.gaogzhen.gmall.realtime.common.app.BaseAPP;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: Administrator
 * @createTime: 2024/05/26 11:08
 */
public class DimAPP extends BaseAPP {

    public static void main(String[] args) {
        new DimAPP().start(8085, 4, "dim_app", "topic_db");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaSource) {
        kafkaSource.print();
    }
}
