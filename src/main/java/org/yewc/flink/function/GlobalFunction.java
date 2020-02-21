package org.yewc.flink.function;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.yewc.flink.entity.*;
import org.yewc.flink.util.DateUtils;
import org.yewc.flink.util.RichJedisReader;
import org.yewc.flink.util.RichJedisWriter;
import scala.Int;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 通用数据聚合函数
 */
public class GlobalFunction extends KeyedProcessFunction<Row, Row, Tuple2> {

    /** 保留最后的水印 */
    private ValueState<Long> waterMarkState;

    /** 窗口数据 */
    private MapState<Long, Object[]> globalWindow;

    /** 空值判断 */
    private ValueState<Boolean> emptyFlag;

    /** 执行窗口 */
    private ValueState<Set> recountWindow;

    /** 上个结果 */
    private ValueState<Object[]> preValue;

    /** 是否保留历史状态，一般没必要，一旦超过总窗口时间就清除 */
    private boolean keepOldData;

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

    /** 0点开始的作业，对于昨天延迟数据会保留多久，毕竟昨天状态可能不全，不要超过windowUnix - 24小时，不填默认保留一个windowSplit */
    private Long keepLateZeroTime;

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

    /** 字段下标索引 */
    private int[] fieldIndexes;

    /** 空值 */
    private Object[] emptyValue;

    /**----------- 以下用于减法操作 -----------*/

    /** 窗口数据，时间-订单 */
    private MapState<String, Set<String>> primaryKeyState;

    /** h key的前缀，可以动态，如field_0 */
    private String hkeyPrefix;

    /** h key的后缀，可以动态，如field_0 */
    private String hkeySuffix;

    /** h key的field，如0 */
    private int hkeyField;

    /** h key的value下标和当前值映射，如0=0;1=1;2=2 */
    private Map<Integer, Integer> hkeyValueMap;

    /** h key的时间field，如0 */
    private int hkeyValueTime;

    /** 详单分割字符 */
    private String splitDetailChar;

    /** jedis reader */
    private static RichJedisReader jedisReader;

    public static GlobalFunction getInstance() {
        return new GlobalFunction();
    }

    public GlobalFunction() {
        this.keyFlag = new HashSet<>(5);

        this.lateness = 0L;
        this.alwaysCalculate = true;
        this.keepOldData = false;
        this.startZeroTime = false;
        this.recountLateData = true;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        waterMarkState = getRuntimeContext().getState(new ValueStateDescriptor<>("waterMarkState", Long.class));
        globalWindow = getRuntimeContext().getMapState(new MapStateDescriptor<>("globalWindow", Long.class, Object[].class));
        emptyFlag = getRuntimeContext().getState(new ValueStateDescriptor<>("emptyFlag", Boolean.class));
        preValue = getRuntimeContext().getState(new ValueStateDescriptor<>("preValue", Object[].class));

        if (recountLateData) {
            recountWindow = getRuntimeContext().getState(new ValueStateDescriptor<>("recountWindow", Set.class));
        }

        if (keepLateZeroTime == null) {
            keepLateZeroTime = windowSlide;
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void processElement(Row row, Context ctx, Collector<Tuple2> out) throws Exception {
        String key = ctx.getCurrentKey().toString();

        valueStateHandler(row);

        if (!keyFlag.contains(key)) {
            Long waterMark = waterMarkState.value();
            long currentWatermark = ctx.timerService().currentWatermark();
            if (currentWatermark < 0) {
                currentWatermark = System.currentTimeMillis();
            }
            currentWatermark = TimeWindow.getWindowStartWithOffset(currentWatermark, 0, windowSlide) + windowSlide;

            if (waterMark == null || waterMark < currentWatermark) {
                waterMark = currentWatermark;
                waterMarkState.update(waterMark);
            }
            // 首次触发
            ctx.timerService().registerProcessingTimeTimer(waterMark + lateness);
            keyFlag.add(key);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2> out) throws Exception {
        String dataKey = ctx.getCurrentKey().toString();
        Long waterMark = waterMarkState.value();

        // 输出数据
        Object[] result = (Object[]) getValue();
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

        // 判断是否需要清理长时间没有数据的旧key
        boolean nextTrigger = true;
        if (!keepOldData) {
            Boolean flag = emptyFlag.value();
            if (flag == null || flag) {
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
            emptyFlag.update(true);
        }

        if (nextTrigger) {
            waterMark += windowSlide;
            long currentWatermark = TimeWindow.getWindowStartWithOffset(ctx.timerService().currentWatermark(), 0, windowSlide) + windowSlide;
            waterMarkState.update(waterMark);
            if (waterMark <= currentWatermark) {
                ctx.timerService().registerProcessingTimeTimer(waterMark + lateness);
            } else {
                // 说明当前水印没有更新，可能是数据延迟了，等下次有数据再启动
                keyFlag.remove(dataKey);
            }
        } else {
            keyEmptyCount.remove(dataKey);
            keyFlag.remove(dataKey);
            waterMarkState.clear();
            globalWindow.clear();
            preValue.clear();
            emptyFlag.clear();
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
            if (typeAndField.length >= 2) {
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
     * 单行操作
     * @param row
     * @throws Exception
     */
    private void valueStateHandler(Row row) throws Exception {
        long time = DateUtils.parse(row.getField(timeField));

        // 0点之后，昨天迟到的数据不再计算，因为昨天24小时状态已清除一部分
        Long waterMark = waterMarkState.value();
        if (startZeroTime && waterMark != null) {
            // 注意0点临界值
            if ((waterMark - time) > keepLateZeroTime
                    && getStartDayTime(waterMark).compareTo(getStartDayTime(time)) > 0) {
                return;
            }
        }

        Long start = TimeWindow.getWindowStartWithOffset(time, 0, windowSlide);
        if (!globalWindow.contains(start)) {
            globalWindow.put(start, cloneEmpty(emptyValue));
        }

        Object[] data = globalWindow.get(start);

        for (int i = 0; i < data.length; i++) {
            Object fieldData = row.getField(fieldIndexes[i]);
            data[i] = handleData(data[i], fieldData, false);
        }
        globalWindow.put(start, data);

        Boolean flag = emptyFlag.value();
        if (flag == null || flag) {
            emptyFlag.update(false);
        }

        // 记录当前及之后的窗口数据
        if (recountLateData) {
            Set theRecount = recountWindow.value();
            if (theRecount == null) {
                theRecount = new HashSet();
            }

            if (!theRecount.contains(start)) {
                theRecount.addAll(getBetweenTime(start, getEndDayTime(start)));
                recountWindow.update(theRecount);
            }
        }
    }

    /**
     * 深复制空值
     * @param os
     * @return
     */
    private Object[] cloneEmpty(Object... os) {
        Object[] newOs = new Object[os.length];
        for (int i = 0; i < os.length; i++) {
            Object temp = os[i];
            if (temp instanceof RoaringBitmap) {
                newOs[i] = new RoaringBitmap();
            } else if (temp instanceof Roaring64NavigableMap) {
                newOs[i] = new Roaring64NavigableMap();
            } else if (temp instanceof HashSet) {
                newOs[i] = new HashSet<Object>();
            } else if (temp instanceof Long) {
                newOs[i] = 0L;
            } else if (temp instanceof Double) {
                newOs[i] = 0.0;
            } else {
                throw new RuntimeException("can not clone this class -> " + temp.getClass());
            }
        }

        return newOs;
    }

    /**
     * 处理数据
     * @param oldData
     * @param newData
     * @param batch
     * @return
     */
    private Object handleData(Object oldData, Object newData, boolean batch) {
        if (newData != null) {
            if (oldData instanceof RoaringBitmap) {
                if (batch) {
                    ((RoaringBitmap) oldData).or((RoaringBitmap) newData);
                } else {
                    ((RoaringBitmap) oldData).add((Integer) newData);
                }
            } else if (oldData instanceof Roaring64NavigableMap) {
                if (batch) {
                    ((Roaring64NavigableMap) oldData).or((Roaring64NavigableMap) newData);
                } else {
                    ((Roaring64NavigableMap) oldData).add((Long) newData);
                }
            } else if (oldData instanceof HashSet) {
                if (batch) {
                    ((HashSet) oldData).addAll((HashSet) newData);
                } else {
                    ((HashSet) oldData).add(newData);
                }
            } else if (oldData instanceof Long) {
                if (batch) {
                    oldData = ((Long) oldData) + new Long(newData.toString());
                } else {
                    oldData = ((Long) oldData) + 1;
                }
            } else if (oldData instanceof Double) {
                if (batch) {
                    oldData = ((Double) oldData) + ((Double) newData);
                } else {
                    oldData = ((Double) oldData) + Double.valueOf(newData.toString());
                }
            } else {
                throw new RuntimeException("can not handle this class -> " + oldData.getClass());
            }
        }
        return oldData;
    }

    /**
     * 缩减数据
     * @param oldData
     * @param newData
     * @return
     */
    private Object reduceData(Object oldData, Object newData, Boolean reduce) {
        if (newData != null) {
            if (oldData instanceof RoaringBitmap) {
                if (reduce) {
                    ((RoaringBitmap) oldData).remove((Integer) newData);
                }
            } else if (oldData instanceof Roaring64NavigableMap) {
                if (reduce) {
                    ((Roaring64NavigableMap) oldData).removeLong((Long) newData);
                }
            } else if (oldData instanceof HashSet) {
                if (reduce) {
                    ((HashSet) oldData).remove(newData);
                }
            } else if (oldData instanceof Long) {
                oldData = ((Long) oldData) - 1;
            } else if (oldData instanceof Double) {
                oldData = ((Double) oldData) - Double.valueOf(newData.toString());
            } else {
                throw new RuntimeException("can not handle this class -> " + oldData.getClass());
            }
        }
        return oldData;
    }

    /**
     * 不管有没新数据都输出
     * @return
     * @throws Exception
     */
    public Object getValue() throws Exception {

        Set theRecountWindow = recountWindow.value();
        Map<Long, Object[]> theGlobalWindow = windowToMap();
        Long lastWindow = waterMarkState.value();
        Object[] value = preValue.value();

        // 如果没有最新分片的数据和要去除的数据，直接返回缓存值
        if (emptyFlag.value()) {
            if (alwaysCalculate) {
                // 当前空窗口数据也输出
                if (value != null && !theGlobalWindow.containsKey(lastWindow - windowSlide) &&
                        !theGlobalWindow.containsKey(lastWindow - windowSlide * (windowSplit + 1))) {
                    if (!recountLateData) {
                        return value;
                    }

                    // 重计算模式下value会有历史，需要重跑
                    theRecountWindow.add(lastWindow);
                }
            } else {
                if (!theGlobalWindow.containsKey(lastWindow - windowSlide) &&
                        !theGlobalWindow.containsKey(lastWindow - windowSlide * (windowSplit + 1))) {
                    return null;
                }
            }
        } else if (recountLateData) {
            theRecountWindow.add(lastWindow);
        }

        Object[] valueTemp;
        if (!recountLateData) {
            valueTemp = toCountData(lastWindow, theGlobalWindow);
        } else {
            valueTemp = new Object[theRecountWindow.size()];
            Iterator<Long> iter = theRecountWindow.iterator();
            int i = 0;
            Long window;
            Object[] temp;
            while (iter.hasNext()) {
                window = iter.next();
                temp = new Object[2];
                temp[0] = window;
                temp[1] = toCountData(window, theGlobalWindow);
                valueTemp[i] = temp;
                i++;
            }
            recountWindow.clear();
        }

        if (alwaysCalculate) {
            preValue.update(valueTemp);
        }

        globalWindow.clear();
        globalWindow.putAll(theGlobalWindow);
        return valueTemp;
    }

    private Object[] toCountData(Long currentWindow, Map<Long, Object[]> theGlobalWindow) throws Exception {
        Object[] tempValue = cloneEmpty(emptyValue);

        Long start = (startZeroTime ? getStartDayTime(currentWindow - 1) : null);
        Long end = (startZeroTime ? getEndDayTime(currentWindow - 1) : null);
        List<Long> removeKey = new ArrayList<>();
        Iterator<Map.Entry<Long, Object[]>> item = theGlobalWindow.entrySet().iterator();
        while (item.hasNext()) {
            Map.Entry<Long, Object[]> kv = item.next();
            Long key = kv.getKey();

            // 获取每个分片是否在总窗口范围内，超出或未来的均忽略
            int bt = (int) ((currentWindow - key)/windowSlide);
            if (bt >= 1 && bt <= windowSplit) {
                if (!startZeroTime || (key >= start && key <= end)) {
                    Object[] data = kv.getValue();
                    for (int i = 0; i < data.length; i++) {
                        tempValue[i] = handleData(tempValue[i], data[i], true);
                    }
                }
            } else if (bt > windowSplit) {
                removeKey.add(key);
            }
        }

        for (int i = 0; i < removeKey.size(); i++) {
            theGlobalWindow.remove(removeKey.get(i));
        }

        Object[] value = new Object[tempValue.length];
        for (int i = 0; i < value.length; i++) {
            Object result = tempValue[i];
            if (result instanceof RoaringBitmap) {
                value[i] = ((RoaringBitmap) result).getLongCardinality();
            } else if (result instanceof Roaring64NavigableMap) {
                value[i] = ((Roaring64NavigableMap) result).getLongCardinality();
            } else if (result instanceof HashSet) {
                value[i] = new Long(((HashSet) result).size());
            } else if (result instanceof Long) {
                value[i] = result;
            } else if (result instanceof Double) {
                value[i] = result;
            } else {
                throw new RuntimeException("can not get value this class -> " + result.getClass());
            }
        }

        return value;
    }

    private Map<Long, Object[]> windowToMap() throws Exception {
        Map<Long, Object[]> result = new HashMap<>();

        Iterable<Map.Entry<Long, Object[]>> iter = globalWindow.entries();
        for (Map.Entry<Long, Object[]> entry : iter) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    /**
     * 获取窗口当天开始时间
     * @param time
     * @return
     * @throws Exception
     */
    private Long getStartDayTime(Long time) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.parse(sdf.format(new Date(time))).getTime();
    }

    /**
     * 获取窗口当天结束时间
     * @param time
     * @return
     * @throws Exception
     */
    private Long getEndDayTime(Long time) throws Exception {
        return getStartDayTime(time) + 86400000 - windowSlide;
    }

    /**
     * 根据分割时间，获取两个时间范围列表值
     * @param start
     * @param end
     * @return
     * @throws Exception
     */
    private List<Long> getBetweenTime(Long start, Long end) throws Exception {
        Long lastWindow = waterMarkState.value();

        List<Long> result = new ArrayList<>();
        int size = Long.valueOf((end - start)/windowSlide).intValue() + 1;
        for (int i = 1; i <= size; i++) {
            Long temp = start + windowSlide*i;
            if (lastWindow == null) {
                if (temp.compareTo(new Date().getTime()) > 0) {
                    continue;
                }
            } else if (temp.compareTo(lastWindow) > 0) {
                continue;
            }

            result.add(temp);
        }
        return result;
    }

    public GlobalFunction setKeepOldData(boolean keepOldData) {
        this.keepOldData = keepOldData;
        if (!keepOldData) {
            keyEmptyCount = new ConcurrentHashMap<>();
        }
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
        groupKey = groupSchema.getJSONArray("group");
        fieldKey = groupSchema.getJSONArray("field");

        this.emptyValue = new Object[groupKey.size()];
        this.fieldIndexes = new int[groupKey.size()];
        for (int i = 0; i < emptyValue.length; i++) {
            String[] temp = groupKey.getString(i).split("_");
            String groupType = temp[0];
            String fieldType = null;
            int fieldIndex;
            if (temp.length == 2) {
                fieldIndex = Integer.valueOf(temp[1]);
            } else {
                fieldType = temp[1];
                fieldIndex = Integer.valueOf(temp[2]);
            }
            fieldIndexes[i] = fieldIndex;

            switch (groupType) {
                case "distinct":
                    if ("int".equals(fieldType)) {
                        emptyValue[i] = new RoaringBitmap();
                    } else if ("long".equals(fieldType)) {
                        emptyValue[i] = new Roaring64NavigableMap();
                    } else {
                        emptyValue[i] = new HashSet<Object>();
                    }
                    break;
                case "count":
                    emptyValue[i] = 0L;
                    break;
                case "sum":
                    emptyValue[i] = 0.0;
                    break;
                default:
                    throw new RuntimeException("unmatch group type of <" + groupType + ">");
            }
        }
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

    public GlobalFunction setKeepLateZeroTime(Long keepLateZeroTime) {
        this.keepLateZeroTime = keepLateZeroTime;
        return this;
    }

    public GlobalFunction setRedisUtil(String address, int port, String passwd) {
        jedisReader = new RichJedisReader(address, port, passwd);
        return this;
    }

    public GlobalFunction setReduceInfo(String hkeyPrefix, String hkeySuffix,
                                        int hkeyField, int hkeyValueTime, String hkeyValueString, String splitDetailChar) {
        this.hkeyPrefix = hkeyPrefix;
        this.hkeySuffix = hkeySuffix;
        this.hkeyField = hkeyField;
        this.hkeyValueTime = hkeyValueTime;
        this.splitDetailChar = splitDetailChar;

        String[] temp = hkeyValueString.split(";");
        this.hkeyValueMap = new HashMap<>(temp.length);
        for (int i = 0; i < temp.length; i++) {
            hkeyValueMap.put(Integer.valueOf(temp[i].split("=")[0]),
                    Integer.valueOf(temp[i].split("=")[1]));
        }
        return this;
    }

}

