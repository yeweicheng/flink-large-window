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
import org.yewc.flink.entity.*;
import org.yewc.flink.util.DateUtils;

import java.util.*;

public class CenterFunction extends KeyedProcessFunction<Row, Row, Row> {

    private Map<String, ValueState<ValueData>> stateMap;
    private ValueState<List> buffer;
    private ValueState<Long> waterMarkState;

    private boolean keepOldData;
    private Long windowUnix;
    private Long windowSlide;
    private Long lateness;
    private int windowSplit;
    private int timeField;
    private JSONObject groupSchema;
    private JSONArray groupKey;
    private JSONArray fieldKey;

    private Set<String> keyFlag;
    private Map<String, Integer> keyEmptyCount;

    public CenterFunction(boolean keepOldData, Long windowUnix, Long windowSlide, int timeField, String groupString) {
        this(keepOldData, windowUnix, windowSlide, 0L, timeField, groupString);
    }

    public CenterFunction(boolean keepOldData, Long windowUnix, Long windowSlide, Long lateness, int timeField, String groupString) {
        this.keepOldData = keepOldData;
        this.windowUnix = windowUnix;
        this.windowSlide = windowSlide;
        this.lateness = lateness == null ? 0L : lateness;
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
        ec.registerPojoType(DistinctIntValueData.class);
        ec.registerPojoType(DistinctLongValueData.class);
        ec.registerPojoType(DistinctObjectValueData.class);
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

        fieldKey = groupSchema.getJSONArray("field");

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
        int groupSize = groupKey.size();
        int fieldSize = fieldKey.size();
        Row row = new Row(fieldSize + groupSize);
        for (int i = 0; i < fieldSize; i++) {
            String[] typeAndField = fieldKey.getString(i).split("_");
            String type = typeAndField[0];
            int field = -1;
            if (typeAndField.length == 2) {
                field = Integer.valueOf(typeAndField[1]);
            }

            switch (type) {
                case "key":
                    if (field == -1) {
                        row.setField(i, dataKey);
                    } else {
                        row.setField(i, ctx.getCurrentKey().getField(field));
                    }
                    break;
                case "starttime":
                    if (field == 10) {
                        row.setField(i, (waterMark - windowUnix)/1000);
                    } else if (field == 13) {
                        row.setField(i, waterMark - windowUnix);
                    } else {
                        row.setField(i, DateUtils.format((waterMark - windowUnix)/1000));
                    }
                    break;
                case "endtime":
                    if (field == 10) {
                        row.setField(i, waterMark/1000 - 1);
                    } else if (field == 13) {
                        row.setField(i, waterMark - 1);
                    } else {
                        row.setField(i, DateUtils.format(waterMark/1000 - 1));
                    }
                    break;
                default:
                    throw new RuntimeException("unmatch key type");
            }
        }
        for (int i = 0; i < groupSize; i++) {
            row.setField(i + fieldSize, stateMap.get(groupKey.getString(i)).value().getValue());
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
            ctx.timerService().registerProcessingTimeTimer(waterMark + lateness);
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

    /**
     * 首次触发，不做输出操作
     * @param ctx
     * @return
     * @throws Exception
     */
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
            ctx.timerService().registerProcessingTimeTimer(waterMark + lateness);
            return true;
        }
        return false;
    }

    /**
     * 处理多类state
     * @param key
     * @param current
     * @param elements
     * @throws Exception
     */
    private void valueStateHandler(String key, ValueData current, List elements) throws Exception {
        if (current == null) {
            String[] temp = key.split("_");
            String groupType = temp[0];
            String fieldType = null;
            int fieldIndex;
            if (temp.length == 2) {
                fieldIndex = Integer.valueOf(temp[1]);
            } else {
                fieldType = temp[1];
                fieldIndex = Integer.valueOf(temp[2]);
            }
            switch (groupType) {
                case "distinct":
                    if ("int".equals(fieldType)) {
                        current = new DistinctIntValueData(windowUnix, windowSlide, timeField, fieldIndex);
                    } else if ("long".equals(fieldType)) {
                        current = new DistinctLongValueData(windowUnix, windowSlide, timeField, fieldIndex);
                    } else {
                        current = new DistinctObjectValueData(windowUnix, windowSlide, timeField, fieldIndex);
                    }
                    break;
                case "count":
                    current = new CountValueData(windowUnix, windowSlide, timeField, fieldIndex);
                    break;
                case "sum":
                    current = new SumValueData(windowUnix, windowSlide, timeField, fieldIndex);
                    break;
                default:
                    throw new RuntimeException("unmatch group type of <" + groupType + ">");
            }
        }

        current.lastWindow = waterMarkState.value();
        current.putElements(elements);
        stateMap.get(key).update(current);
    }
}

