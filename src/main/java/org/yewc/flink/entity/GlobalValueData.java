package org.yewc.flink.entity;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.yewc.flink.util.DateUtils;

import java.util.*;

public class GlobalValueData {

    public Long lastWindow;
    public Long windowUnix;
    public Long windowSlide;
    public Long lateness;
    public int windowSplit;
    public int timeField;
    private Object[] emptyValue;
    private boolean emptyFlag;
    private int[] fieldIndexes;
    private Map<Long, Object[]> globalWindow;

    public GlobalValueData(Long windowUnix, Long windowSlide, Long lateness, int timeField, JSONArray groupKey) {
        this.windowUnix = windowUnix;
        this.windowSlide = windowSlide;
        this.lateness = lateness;
        this.timeField = timeField;
        this.windowSplit = new Long(windowUnix/windowSlide).intValue();

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

    public GlobalValueData(Long windowUnix, Long windowSlide, int timeField, JSONArray groupKey) {
        this(windowUnix, windowSlide, 0L, timeField, groupKey);
    }

    public void putElement(Row row) throws Exception {
        long time = DateUtils.parse(row.getField(timeField));
        long start = TimeWindow.getWindowStartWithOffset(time, 0, windowSlide);
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
    }

    public void putElements(Iterable<Row> elements) throws Exception {
        Iterator<Row> eleIter = elements.iterator();
        while (eleIter.hasNext()) {
            putElement(eleIter.next());
        }
    }

    public Object getValue() throws Exception {
        Object[] value = cloneEmpty(emptyValue);

        List<Long> removeKey = new ArrayList<>();
        Iterator<Map.Entry<Long, Object[]>> item = globalWindow.entrySet().iterator();
        while (item.hasNext()) {
            Map.Entry<Long, Object[]> kv = item.next();
            Long key = kv.getKey();
            int bt = (int) ((lastWindow - key)/windowSlide);
            if (bt >= 1 && bt <= windowSplit) {
                Object[] data = kv.getValue();
                for (int i = 0; i < data.length; i++) {
                    value[i] = handleData(value[i], data[i], true);
                }
            } else if (bt > windowSplit) {
                removeKey.add(key);
            }
        }

        for (int i = 0; i < removeKey.size(); i++) {
            globalWindow.remove(removeKey.get(i));
        }

        Object[] data = new Object[value.length];
        for (int i = 0; i < data.length; i++) {
            Object result = value[i];
            if (result instanceof RoaringBitmap) {
                data[i] = ((RoaringBitmap) result).getLongCardinality();
            } else if (result instanceof Roaring64NavigableMap) {
                data[i] = ((Roaring64NavigableMap) result).getLongCardinality();
            } else if (result instanceof HashSet) {
                data[i] = new Long(((HashSet) result).size());
            } else if (result instanceof Long) {
                data[i] = result;
            } else if (result instanceof Double) {
                data[i] = result;
            } else {
                throw new RuntimeException("can not get value this class -> " + result.getClass());
            }
        }
        return data;
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

    public boolean isEmptyFlag() {
        return emptyFlag;
    }

    public void setEmptyFlag(boolean emptyFlag) {
        this.emptyFlag = emptyFlag;
    }
}
