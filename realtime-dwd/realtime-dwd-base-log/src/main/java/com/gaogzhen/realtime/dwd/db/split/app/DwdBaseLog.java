package com.gaogzhen.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gaogzhen.gmall.realtime.common.app.BaseAPP;
import com.gaogzhen.gmall.realtime.common.constant.Constant;
import com.gaogzhen.gmall.realtime.common.util.DateFormatUtil;
import com.gaogzhen.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author gaogzhen
 * @date 2024/6/11 20:48
 */
public class DwdBaseLog extends BaseAPP {
    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心业务处理
        // 1.ETL过滤不完整的数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);
        // 2.新旧访客修复
        KeyedStream<JSONObject, String> keyedStream = keyByWithWaterMark(jsonObjStream);

        SingleOutputStreamOperator<JSONObject> isNewFixStream = isNewFix(keyedStream);
        // 4.拆分不同类型的行为日志
        // 启动日志：启动消息 报错消息
        // 页面日志：页面信息 曝光信息 动作信息 报错信息
        OutputTag<String> startTag = new OutputTag<>("start", TypeInformation.of(String.class));
        OutputTag<String> errorTag = new OutputTag<>("error", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<>("display", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<>("action", TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> pageStream = splitLog(isNewFixStream, startTag, errorTag, displayTag, actionTag);
        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> errorStream = pageStream.getSideOutput(errorTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);

        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        startStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        errorStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        displayStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));

    }

    private static SingleOutputStreamOperator<String> splitLog(SingleOutputStreamOperator<JSONObject> isNewFixStream, OutputTag<String> startTag, OutputTag<String> errorTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        return isNewFixStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject val, Context context, Collector<String> out) throws Exception {
                System.out.println(val);
                // 根据数据的不同 拆分到不同的侧输出流
                JSONObject err = val.getJSONObject("err");
                if (err != null) {
                    context.output(errorTag, err.toJSONString());
                    val.remove("err");
                }
                JSONObject page = val.getJSONObject("page");
                JSONObject start = val.getJSONObject("start");
                JSONObject common = val.getJSONObject("common");
                Long ts = val.getLong("ts");

                if (start != null) {
                    // 启动日志
                    context.output(startTag, val.toJSONString());
                } else if (page != null) {
                    // 页面日志
                    JSONArray displays = val.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page", page);
                            context.output(displayTag, display.toJSONString());
                        }
                        val.remove("displays");
                    }
                    // 动作
                    JSONArray actions = val.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page", page);
                            context.output(actionTag, action.toJSONString());
                        }
                        val.remove("actions");
                    }

                    //  只保留page信息 写出到主流
                    out.collect(val.toJSONString());
                } else {
                    // 留空
                }
            }
        });
    }

    private static SingleOutputStreamOperator<JSONObject> isNewFix(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<String> firstLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 创建状态
                firstLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first_login_dt", String.class));
            }

            @Override
            public void processElement(JSONObject val, Context context, Collector<JSONObject> out) throws Exception {
                // System.out.println(val);
                // 1.获取当前数据的is_new
                JSONObject common = val.getJSONObject("common");
                String isNew = common.getString("is_new");
                String firstLoginDt = firstLoginDtState.value();
                Long ts = val.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                if ("1".equals(isNew)) {
                    // 判断当前状态情况
                    if (firstLoginDt != null && !firstLoginDt.equals(curDt)) {
                        // 状态不为空 且日期不是当天
                        common.put("is_new", "0");
                    } else if (firstLoginDt == null) {
                        // 状态为空
                        firstLoginDtState.update(curDt);
                    } else {
                        // 新访客当天重复访问
                    }
                } else if ("0".equals(isNew)) {
                    if (firstLoginDt == null) {
                        // 老用户 flink实时数仓未记录
                        firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                    } else {
                        // 正常情况
                    }
                } else {
                    // 错误数据
                }
                out.collect(val);
            }
        });
    }

    private static KeyedStream<JSONObject, String> keyByWithWaterMark(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long recordTimestamp) {
                        return jsonObject.getLong("ts");
                    }
                })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {

                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
    }

    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String val, Collector<JSONObject> out) throws Exception {
                // System.out.println(val);
                try {
                    JSONObject jsonObject = JSONObject.parseObject(val);
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject start = jsonObject.getJSONObject("start");
                    JSONObject common = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");
                    if (ts != null && common != null && common.getString("mid") != null) {
                        if (page != null || start != null) {
                            out.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("过滤掉脏数据：" + val);
                }
            }
        });
    }

}
