package org.yewc.flink.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.yewc.flink.entity.*;
import org.yewc.flink.util.DateUtils;

import java.util.*;

/**
 * 通用数据聚合函数
 */
public class GlobalFunction extends KeyedProcessFunction<Row, Row, Tuple2> implements CheckpointedFunction {

    /** 保留预聚合数据 */
    private ValueState<GlobalValueData> valueData;

    /** 保留缓存数据，只用在batch窗口中 */
    private ValueState<List> buffer;

    /** 保留最后的水印 */
    private ValueState<Long> waterMarkState;

    /** 保留预聚合数据 - 临时 */
    private GlobalValueData valueDataTemp;

    /** 保留缓存数据，只用在batch窗口中 - 临时 */
    private List bufferTemp;

    /** 是否保留历史状态，一般没必要，一旦超过总窗口时间就清除 */
    private boolean keepOldData;

    /** batch窗口聚合，简单来说时间点到了再存放，而不是来一条就存放 */
    private boolean batch;

    /** 是否总是计算，不管当前滑动是否没有数据流入 */
    private boolean alwaysCalculate;

    /** 总窗口大小，ms */
    private Long windowUnix;

    /** 滑动窗口大小，ms */
    private Long windowSlide;

    /** 延迟时长，ms */
    private Long lateness;

    /** 总窗口大小/滑动窗口大小 */
    private int windowSplit;

    /** 时间字段在row中的位置 */
    private int timeField;

    /** 是否从0点开始 */
    private boolean startZeroTime;

    /** 是否重算过期数据所包括的窗口值 */
    private boolean recountLateData;

    /** 聚合元数据 */
    private JSONObject groupSchema;

    /** 聚合元数据，key字段 */
    private JSONArray groupKey;

    /** 聚合元数据，统计字段 */
    private JSONArray fieldKey;

    /** 用于判断key是否触发过timer */
    private Set<String> keyFlag;

    /** 记录key空值滑动次数，用于清理状态 */
    private Map<String, Integer> keyEmptyCount;

    public static GlobalFunction getInstance() {
        return new GlobalFunction();
    }

    public GlobalFunction() {
        this.keyFlag = new HashSet<>(5);
        this.lateness = 0L;
        this.batch = false;
        this.alwaysCalculate = true;
        this.keepOldData = false;
        this.startZeroTime = false;
            this.recountLateData = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        groupKey = groupSchema.getJSONArray("group");
        fieldKey = groupSchema.getJSONArray("field");

        valueData = getRuntimeContext().getState(new ValueStateDescriptor<>("valueState_global", GlobalValueData.class));
        buffer = getRuntimeContext().getState(new ValueStateDescriptor<>("bufferState", List.class));
        waterMarkState = getRuntimeContext().getState(new ValueStateDescriptor<>("waterMarkState", Long.class));
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void processElement(Row row, Context ctx, Collector<Tuple2> out) throws Exception {
        if (batch) {
            if (bufferTemp == null) {
                bufferTemp = buffer.value();
                if (bufferTemp == null) {
                    bufferTemp = new ArrayList<Row>();
                }
            }

            bufferTemp.add(row);
        } else {
            valueStateHandler(row);
        }

        if (!keyFlag.contains(ctx.getCurrentKey().toString())) {
            Long waterMark = waterMarkState.value();
            if (waterMark == null) {
                waterMark = TimeWindow.getWindowStartWithOffset(DateUtils.parse(row.getField(timeField)), 0, windowSlide) + windowSlide;
                waterMarkState.update(waterMark);
            }
            // 首次触发
            ctx.timerService().registerProcessingTimeTimer(waterMark + lateness);
            keyFlag.add(ctx.getCurrentKey().toString());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2> out) throws Exception {

        // 遍历需要聚合数据字段
        List elements = bufferTemp;
        GlobalValueData current = valueStateHandler(elements);

        // 输出数据
        Long waterMark = waterMarkState.value();
        String dataKey = ctx.getCurrentKey().toString();
        Object[] result = (Object[]) current.getValue(alwaysCalculate, startZeroTime);
        if (result != null) {
            if (recountLateData) {
                Object[] temp;
                for (int i = 0; i < result.length; i++) {
                    temp = (Object[]) result[i];
                    collectData((Long) temp[0], dataKey, (Object[]) temp[1], ctx, out);
                }
            } else {
                collectData(waterMark, dataKey, result, ctx, out);
            }
        }
        waterMark += windowSlide;
        waterMarkState.update(waterMark);

        // 判断是否需要清理长时间没有数据的旧key
        boolean nextTrigger = true;
        if (!keepOldData) {
            if (current.isEmptyFlag()) {
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
            current.setEmptyFlag(true);
        }
        valueData.update(current);

        if (batch) {
            elements.clear();
            buffer.update(elements);
        }

//        System.out.println("onTimer trigger time: " + (waterMark));
        if (nextTrigger) {
            ctx.timerService().registerProcessingTimeTimer(waterMark + lateness);
        } else {
            keyEmptyCount.remove(dataKey);
            keyFlag.remove(dataKey);

            waterMarkState.update(null);
            valueData.update(null);

            if (batch) {
                buffer.update(null);
            }
        }
    }

    /**
     * 收集数据
     * @param waterMark
     * @param dataKey
     * @param result
     * @param ctx
     * @param out
     * @throws Exception
     */
    private void collectData(Long waterMark, String dataKey, Object[] result,
                             OnTimerContext ctx, Collector<Tuple2> out) throws Exception {
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

        for (int i = 0; i < result.length; i++) {
            row.setField(i + fieldSize, result[i]);
        }
        out.collect(Tuple2.of(true, row));
    }

    /**
     * 处理多类state
     * @param elements
     * @throws Exception
     */
    private GlobalValueData valueStateHandler(List elements) throws Exception {
        if (valueDataTemp == null) {
            valueDataTemp = valueData.value();
            if (valueDataTemp == null) {
                valueDataTemp = new GlobalValueData(windowUnix, windowSlide, timeField, groupKey, recountLateData);
            }
        }

        valueDataTemp.lastWindow = waterMarkState.value();
        if (batch) {
            valueDataTemp.putElements(elements);
        }
        return valueDataTemp;
    }

    private void valueStateHandler(Row row) throws Exception {
        if (valueDataTemp == null) {
            valueDataTemp = valueData.value();
            if (valueDataTemp == null) {
                valueDataTemp = new GlobalValueData(windowUnix, windowSlide, timeField, groupKey, recountLateData);
            }
        }

        valueDataTemp.putElement(row);
    }

    public GlobalFunction setKeepOldData(boolean keepOldData) {
        this.keepOldData = keepOldData;
        if (!keepOldData) {
            keyEmptyCount = new HashMap<>();
        }
        return this;
    }

    public GlobalFunction setBatch(boolean batch) {
        this.batch = batch;
        return this;
    }

    public GlobalFunction setAlwaysCalculate(boolean alwaysCalculate) {
        this.alwaysCalculate = alwaysCalculate;
        return this;
    }

    public GlobalFunction setLateness(Long lateness) {
        this.lateness = lateness == null ? 0L : lateness;
        return this;
    }

    public GlobalFunction setWindowSplit(Long windowSlide, Long windowUnix) {
        this.windowSlide = windowSlide;
        this.windowUnix = windowUnix;
        this.windowSplit = new Long(windowUnix/windowSlide).intValue();
        return this;
    }

    public GlobalFunction setTimeField(int timeField) {
        this.timeField = timeField;
        return this;
    }

    public GlobalFunction setGroupSchema(String groupString) {
        this.groupSchema = JSONObject.parseObject(groupString);
        return this;
    }

    public GlobalFunction setStartZeroTime(boolean startZeroTime) {
        this.startZeroTime = startZeroTime;
        return this;
    }

    public GlobalFunction setRecountLateData(boolean recountLateData) {
        this.recountLateData = recountLateData;
        return this;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        buffer.update(bufferTemp);
        valueData.update(valueDataTemp);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 已经在open初始化了
    }
}

