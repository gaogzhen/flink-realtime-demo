package com.gaogzhen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: Administrator
 * @createTime: 2024/05/26 09:33
 */
public class Test01 {
    public static void main(String[] args) {
        // 1.构建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        // 2.设置检查点和状态后端参数
        // env.enableCheckpointing(5000L);
        // env.setStateBackend(new HashMapStateBackend());
        // 3.读取数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers("node1:9092")
                        .setTopics("topic_db")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setGroupId("test01")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build(),
                WatermarkStrategy.noWatermarks(),
                "kafka_source"
        );
        // 4.对数据源进行处理
        kafkaSource.print();
        // 5.执行环境
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
