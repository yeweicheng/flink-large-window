package org.yewc.flink.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.yewc.flink.entity.CountValueData;
import org.yewc.flink.entity.DistinctValueData;
import org.yewc.flink.entity.SumValueData;
import org.yewc.flink.entity.ValueData;
import org.yewc.flink.util.DateUtils;

import java.util.*;

public class CenterFunction extends KeyedProcessFunction<Row, Row, Row> {

    private Map<String, ValueState<ValueData>> stateMap;
    private ValueState<List> buffer;
    private ValueState<Long> waterMarkState;

    private boolean keepOldData;
    private Long windowUnix;
    private Long windowSlide;
    private int windowSplit;
    private int timeField;
    private JSONObject groupSchema;
    private JSONArray groupKey;

    private Set<String> keyFlag;
    private Map<String, Integer> keyEmptyCount;

    public CenterFunction(boolean keepOldData, Long windowUnix, Long windowSlide, int timeField, String groupString) {
        this.keepOldData = keepOldData;
        this.windowUnix = windowUnix;
        this.windowSlide = windowSlide;
        this.windowSplit = new Long(windowUnix/windowSlide).intValue();
        this.timeField = timeField;
        this.groupSchema = JSONObject.parseObject(groupString);
        this.keyFlag = new HashSet<>(5);

        if (!keepOldData) {
            keyEmptyCount = new HashMap<>();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ExecutionConfig ec = getRuntimeContext().getExecutionConfig();
        ec.registerPojoType(DistinctValueData.class);
        ec.registerPojoType(CountValueData.class);
        ec.registerPojoType(SumValueData.class);

        stateMap = new HashMap<>(1);
        groupKey = groupSchema.getJSONArray("group");
        for (int i = 0; i < groupKey.size(); i++) {
            String typeAndField = groupKey.getString(i);

            ValueState<ValueData> state = getRuntimeContext().getState(new ValueStateDescriptor<>(
                    "valueState_" + typeAndField, ValueData.class));
            stateMap.put(typeAndField, state);
        }

        buffer = getRuntimeContext().getState(new ValueStateDescriptor<>("bufferState", List.class));
        waterMarkState = getRuntimeContext().getState(new ValueStateDescriptor<>("waterMarkState", Long.class));
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void processElement(Row tuple, Context ctx, Collector<Row> out) throws Exception {
        List elements = buffer.value();
        if (elements == null) {
            elements = new ArrayList<Row>(5);
        }

        elements.add(tuple);
        buffer.update(elements);

        if (!keyFlag.contains(ctx.getCurrentKey().toString())) {
            Long waterMark = waterMarkState.value();
            if (waterMark == null) {
                waterMark = ctx.timerService().currentWatermark() > 0 ? ctx.timerService().currentWatermark() : 0;
            }

            ctx.timerService().registerProcessingTimeTimer(waterMark);
            keyFlag.add(ctx.getCurrentKey().toString());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Row> out) throws Exception {

        if (fristTrigger(ctx)) {
            return;
        }

        // 遍历需要聚合数据字段
        List elements = buffer.value();
        for (String groupKey : stateMap.keySet()) {
            valueStateHandler(groupKey, stateMap.get(groupKey).value(), elements);
        }

        // 输出数据
        Long waterMark = waterMarkState.value();
        String dataKey = ctx.getCurrentKey().toString();
        int size = groupKey.size();
        Row row = new Row(2 + size);
        row.setField(0, DateUtils.format(waterMark/1000 - 1));
        row.setField(1, dataKey);
        for (int i = 0; i < size; i++) {
            row.setField(i + 2, stateMap.get(groupKey.getString(i)).value().getValue());
        }
        out.collect(row);
        waterMark += windowSlide;
        waterMarkState.update(waterMark);

        // 判断是否需要清理长时间没有数据的旧key
        boolean nextTrigger = true;
        if (!keepOldData) {
            if (elements.isEmpty()) {
                if (!keyEmptyCount.containsKey(dataKey)) {
                    keyEmptyCount.put(dataKey, 0);
                }

                keyEmptyCount.put(dataKey, keyEmptyCount.get(dataKey) + 1);

                if (keyEmptyCount.get(dataKey) == (windowSplit + 1)) {
                    nextTrigger = false;
                }
            } else {
                keyEmptyCount.remove(dataKey);
            }
        }

        elements.clear();
        buffer.update(elements);

//        System.out.println("onTimer trigger time: " + (waterMark));
        if (nextTrigger) {
            ctx.timerService().registerProcessingTimeTimer(waterMark);
        } else {
            keyEmptyCount.remove(dataKey);
            keyFlag.remove(dataKey);

            buffer.update(null);
            waterMarkState.update(null);
            for (String groupKey : stateMap.keySet()) {
                stateMap.get(groupKey).update(null);
            }
        }
    }

    private boolean fristTrigger(OnTimerContext ctx) throws Exception {
        Long waterMark = waterMarkState.value();
        if (waterMark == null) {
            waterMark = ctx.timerService().currentWatermark() > 0 ? ctx.timerService().currentWatermark() : System.currentTimeMillis();
            waterMark = TimeWindow.getWindowStartWithOffset(waterMark, 0, windowSlide) + windowSlide;
            waterMarkState.update(waterMark);

            // 遍历需要聚合数据字段
            List elements = buffer.value();
            for (String groupKey : stateMap.keySet()) {
                valueStateHandler(groupKey, stateMap.get(groupKey).value(), elements);
            }

            elements.clear();
            buffer.update(elements);

            // 首次触发
            ctx.timerService().registerProcessingTimeTimer(waterMark);
            return true;
        }
        return false;
    }

    private void valueStateHandler(String key, ValueData current, List elements) throws Exception {
        if (current == null) {
            String type = key.split("_")[0];
            int fieldIndex = Integer.valueOf(key.split("_")[1]);
            switch (type) {
                case "distinct":
                    current = new DistinctValueData(windowUnix, windowSlide, timeField, fieldIndex);
                    break;
                case "count":
                    current = new CountValueData(windowUnix, windowSlide, timeField, fieldIndex);
                    break;
                case "sum":
                    current = new SumValueData(windowUnix, windowSlide, timeField, fieldIndex);
                    break;
                default:
                    throw new RuntimeException("unmatch group type of <" + type + ">");
            }
        }

        current.lastWindow = waterMarkState.value();
        current.putElements(elements);
        stateMap.get(key).update(current);
    }
}

