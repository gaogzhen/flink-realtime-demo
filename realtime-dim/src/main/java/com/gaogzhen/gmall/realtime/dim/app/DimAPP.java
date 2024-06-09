package com.gaogzhen.gmall.realtime.dim.app;

import com.alibaba.fastjson.JSONObject;
import com.gaogzhen.gmall.realtime.common.app.BaseAPP;
import com.gaogzhen.gmall.realtime.common.bean.TableProcessDim;
import com.gaogzhen.gmall.realtime.common.constant.Constant;
import com.gaogzhen.gmall.realtime.common.util.FlinkSourceUtil;
import com.gaogzhen.gmall.realtime.common.util.HBaseUtil;
import com.gaogzhen.gmall.realtime.dim.function.DimBroadcastFunction;
import com.gaogzhen.gmall.realtime.dim.function.DimHBaseSinkFunction;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
        // 核心业务逻辑
        // 1.对ods读取的原始数据进行清洗
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(kafkaSource);
        // 2.使用flinkCDC读取配置表数据
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DIM_TABLE_NAME);
        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);

        // 3.在HBase中创建维度表
        SingleOutputStreamOperator<TableProcessDim> createTableStream = createTableStream(mysqlSource);

        // createTableStream.print();
        // 4.做成广播流
        // 广播状态key用于判断是否是维度表，value用于补充信息写出到Hbase
        MapStateDescriptor<String, TableProcessDim> broadcastState = new MapStateDescriptor<>("broadcast_state", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStateStream = createTableStream.broadcast(broadcastState);
        // 5.连接主流和广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectStream(jsonObjStream, broadcastState, broadcastStateStream);
        // 6.筛选出写出的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumnStream = filterColumn(dimStream);
        // 7.写出到hbase
        filterColumnStream.addSink(new DimHBaseSinkFunction());

    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumn(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream) {
        return dimStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> val) throws Exception {
                JSONObject jsonObject = val.f0;
                TableProcessDim dim = val.f1;
                String sinkColumns = dim.getSinkColumns();
                List<String> columns = Arrays.asList(sinkColumns.split(","));
                JSONObject data = jsonObject.getJSONObject("data");
                data.keySet().removeIf(key -> !columns.contains(key));
                return val;
            }
        });
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectStream(SingleOutputStreamOperator<JSONObject> jsonObjStream, MapStateDescriptor<String, TableProcessDim> broadcastState, BroadcastStream<TableProcessDim> broadcastStateStream) {
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectedStream = jsonObjStream.connect(broadcastStateStream);
        return connectedStream.process(
                new DimBroadcastFunction(broadcastState)
        ).setParallelism(1);
    }

    private static SingleOutputStreamOperator<TableProcessDim> createTableStream(DataStreamSource<String> mysqlSource) {
        SingleOutputStreamOperator<TableProcessDim> createTableStream = mysqlSource.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {


            public Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 获取连接
                connection = HBaseUtil.getConnection();
            }

            @Override
            public void close() throws Exception {
                // 关闭连接
                HBaseUtil.closeConnection(connection);
            }

            @Override
            public void flatMap(String val, Collector<TableProcessDim> out) throws Exception {
                // 使用配置表中的数据在Hbase中创建相应的表格
                try {
                    JSONObject jsonObject = JSONObject.parseObject(val);
                    String op = jsonObject.getString("op");

                    TableProcessDim dim;
                    if ("d".equals(op)) {
                        dim = jsonObject.getObject("before", TableProcessDim.class);
                        // 删除表格
                        deleteTable(dim);
                    } else if ("c".equals(op) || "r".equals(op)) {
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        // 创建表格
                        createTable(dim);

                    } else {
                        // 先删除再创建
                        dim = jsonObject.getObject("after", TableProcessDim.class);
                        deleteTable(dim);
                        createTable(dim);
                    }
                    dim.setOp(op);
                    out.collect(dim);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }


            private void createTable(TableProcessDim dim) {
                String sinkFamily = dim.getSinkFamily();
                String[] families = sinkFamily.split(",");
                try {
                    HBaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable(), families);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            private void deleteTable(TableProcessDim dim) {

                try {
                    HBaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, dim.getSinkTable());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        });
        return createTableStream;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaSource) {
        SingleOutputStreamOperator<JSONObject> jsonObjStream = kafkaSource.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String val, Collector<JSONObject> collector) throws Exception {
                try {

                    JSONObject jsonObject = JSONObject.parseObject(val);
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    JSONObject data = jsonObject.getJSONObject("data");
                    if ("gmall".equals(database) && !"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });

        return jsonObjStream;
    }
}
