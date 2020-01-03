package org.yewc.flink.entity;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.yewc.flink.util.DateUtils;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class GlobalValueData {

    public Long lastWindow;
    public Long windowUnix;
    public Long windowSlide;
    public Long lateness;
    public int windowSplit;
    public int timeField;
    private Object[] emptyValue;
    private Object[] value;
    private boolean emptyFlag;
    private int[] fieldIndexes;
    private Map<Long, Object[]> globalWindow;
    private Set<Long> recountWindow;

    public GlobalValueData(Long windowUnix, Long windowSlide, Long lateness, int timeField, JSONArray groupKey, boolean recountLateData) {
        this.windowUnix = windowUnix;
        this.windowSlide = windowSlide;
        this.lateness = lateness;
        this.timeField = timeField;
        this.windowSplit = new Long(windowUnix/windowSlide).intValue();
        this.recountWindow = recountLateData ? new HashSet<>() : null;

        this.globalWindow = new HashMap<>();
        this.emptyFlag = true;
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
    }

    public GlobalValueData(Long windowUnix, Long windowSlide, int timeField, JSONArray groupKey, boolean recountLateData) {
        this(windowUnix, windowSlide, 0L, timeField, groupKey, recountLateData);
    }

    public void putElement(Row row) throws Exception {
        long time = DateUtils.parse(row.getField(timeField));
        Long start = TimeWindow.getWindowStartWithOffset(time, 0, windowSlide);
        if (!globalWindow.containsKey(start)) {
            globalWindow.put(start, cloneEmpty(emptyValue));
        }

        Object[] data = globalWindow.get(start);
        for (int i = 0; i < data.length; i++) {
            Object fieldData = row.getField(fieldIndexes[i]);
            data[i] = handleData(data[i], fieldData, false);
        }

        if (emptyFlag) {
            emptyFlag = false;
        }

        // 记录当前及之后的窗口数据
        if (recountWindow != null) {
            if (!recountWindow.contains(start)) {
                recountWindow.addAll(getBetweenTime(start, getEndDayTime(start)));
            }
        }
    }

    public void putElements(Iterable<Row> elements) throws Exception {
        Iterator<Row> eleIter = elements.iterator();
        while (eleIter.hasNext()) {
            putElement(eleIter.next());
        }
    }

    /**
     * 不管有没新数据都输出
     * @param alwaysCalculate
     * @return
     * @throws Exception
     */
    public Object getValue(boolean alwaysCalculate, boolean startZeroTime) throws Exception {
        // 如果没有最新分片的数据和要去除的数据，直接返回缓存值
        if (emptyFlag) {
            if (alwaysCalculate) {
                // 当前空窗口数据也输出
                if (value != null && !globalWindow.containsKey(lastWindow - windowSlide) &&
                        !globalWindow.containsKey(lastWindow - windowSlide * (windowSplit + 1))) {
                    if (recountWindow == null) {
                        return value;
                    }

                    // 重计算模式下value会有历史，需要重跑
                    recountWindow.add(lastWindow);
                }
            } else {
                if (!globalWindow.containsKey(lastWindow - windowSlide) &&
                        !globalWindow.containsKey(lastWindow - windowSlide * (windowSplit + 1))) {
                    return null;
                }
            }
        } else if (recountWindow != null) {
            recountWindow.add(lastWindow);
        }

        Object[] valueTemp;
        if (recountWindow == null) {
            valueTemp = toCountData(lastWindow, startZeroTime);
        } else {
            valueTemp = new Object[recountWindow.size()];
            Iterator<Long> iter = recountWindow.iterator();
            int i = 0;
            Long window;
            Object[] temp;
            while (iter.hasNext()) {
                window = iter.next();
                temp = new Object[2];
                temp[0] = window;
                temp[1] = toCountData(window, startZeroTime);
                valueTemp[i] = temp;
                i++;
            }
            recountWindow.clear();
        }

        if (alwaysCalculate) {
            this.value = valueTemp;
        }
        return valueTemp;
    }

    private Object[] toCountData(Long currentWindow, boolean startZeroTime) throws Exception {
        Object[] tempValue = cloneEmpty(emptyValue);

        Long start = (startZeroTime ? getStartDayTime(currentWindow - 1) : null);
        Long end = (startZeroTime ? getEndDayTime(currentWindow - 1) : null);
        List<Long> removeKey = new ArrayList<>();
        Iterator<Map.Entry<Long, Object[]>> item = globalWindow.entrySet().iterator();
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
            globalWindow.remove(removeKey.get(i));
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
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.parse(sdf.format(new Date(time))).getTime() + 86400000 - windowSlide;
    }

    private List<Long> getBetweenTime(Long start, Long end) throws Exception {
        List<Long> result = new ArrayList<>();
        int size = Long.valueOf((end - start)/windowSlide).intValue() + 1;
        for (int i = 1; i <= size; i++) {
            Long temp = start + windowSlide*i;
            if (lastWindow == null || temp.compareTo(lastWindow) <= 0) {
                result.add(temp);
            }
        }
        return result;
    }

    public boolean isEmptyFlag() {
        return emptyFlag;
    }

    public void setEmptyFlag(boolean emptyFlag) {
        this.emptyFlag = emptyFlag;
    }

    //    public static void main(String[] args) throws Exception {
//        Long start = (GlobalValueData.getStartDayTime(1577960399999L));
//        Long end = (GlobalValueData.getEndDayTime(1577960399999L));
//
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        System.out.println(sdf.format(new Date(start)));
//        System.out.println(sdf.format(new Date(end)));
//
//        List<Long> result = GlobalValueData.getBetweenTime(start, end);
//        for (int i = 0; i < result.size(); i++) {
//            System.out.println(sdf.format(new Date(result.get(i))));
//        }
//    }
}
