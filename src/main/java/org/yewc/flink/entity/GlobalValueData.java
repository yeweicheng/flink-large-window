package org.yewc.flink.entity;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.yewc.flink.util.DateUtils;

import java.util.*;

public class GlobalValueData extends ValueData {

    public Object[] value;
    public Object[] empty;
    public int[] fieldIndexes;
    public Map<Long, Object[]> globalWindow;

    public GlobalValueData(Long windowUnix, Long windowSlide, Long lateness, int timeField, int theField) {
        super(windowUnix, windowSlide, lateness, timeField, theField);
    }

    public GlobalValueData(Long windowUnix, Long windowSlide, int timeField, int theField) {
        super(windowUnix, windowSlide, 0L, timeField, theField);
    }

    public void init(JSONArray groupKey) {
        this.value = new Object[groupKey.size()];
        this.globalWindow = new HashMap<>();

        this.empty = new Object[groupKey.size()];
        this.fieldIndexes = new int[groupKey.size()];
        for (int i = 0; i < empty.length; i++) {
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
                        empty[i] = new RoaringBitmap();
                    } else if ("long".equals(fieldType)) {
                        empty[i] = new Roaring64NavigableMap();
                    } else {
                        empty[i] = new HashSet<Object>();
                    }
                    break;
                case "count":
                    empty[i] = 0L;
                    break;
                case "sum":
                    empty[i] = 0.0;
                    break;
                default:
                    throw new RuntimeException("unmatch group type of <" + groupType + ">");
            }
        }

    }

    @Override
    public void putElements(Iterable<Row> elements) throws Exception {
        Map<Long, Object[]> smallWindow = new HashMap<>(8);
        Iterator<Row> eleIter = elements.iterator();
        long time;
        long start;
        Row row;
        while (eleIter.hasNext()) {
            row = eleIter.next();
            time = DateUtils.parse(row.getField(timeField));
            start = TimeWindow.getWindowStartWithOffset(time, 0, windowSlide);
            if (!smallWindow.containsKey(start)) {
                smallWindow.put(start, empty.clone());
            }

            Object[] data = smallWindow.get(start);
            for (int i = 0; i < data.length; i++) {
                Object fieldData = row.getField(fieldIndexes[i]);
                if (fieldData != null) {
                    Object result = data[i];
                    if (result instanceof RoaringBitmap) {
                        ((RoaringBitmap) result).add((Integer) fieldData);
                    } else if (result instanceof Roaring64NavigableMap) {
                        ((Roaring64NavigableMap) result).add((Long) fieldData);
                    } else if (result instanceof Long) {
                        data[i] = ((Long) result) + 1;
                    } else if (result instanceof Double) {
                        data[i] = ((Double) result) + Double.valueOf(fieldData.toString());
                    }
                }
            }
        }

        Iterator<Map.Entry<Long, Object[]>> item = smallWindow.entrySet().iterator();
        while(item.hasNext()){
            Map.Entry<Long, Object[]> kv = item.next();
            Long key = kv.getKey();
            Object[] data = kv.getValue();
            if (globalWindow.containsKey(key)) {
                Object[] globalData = globalWindow.get(key);
                for (int i = 0; i < data.length; i++) {
                    Object result = data[i];
                    if (result instanceof RoaringBitmap) {
                        ((RoaringBitmap) globalData[i]).or((RoaringBitmap) result);
                    } else if (result instanceof Roaring64NavigableMap) {
                        ((Roaring64NavigableMap) globalData[i]).or((Roaring64NavigableMap) result);
                    } else if (result instanceof Long) {
                        globalData[i] = ((Long) globalData[i]) + ((Long) result);
                    } else if (result instanceof Double) {
                        globalData[i] = ((Double) globalData[i]) + ((Double) result);
                    }
                }
            } else {
                globalWindow.put(key, data);
            }
        }

        value = empty.clone();

        List<Long> removeKey = new ArrayList<>();
        item = globalWindow.entrySet().iterator();
        while (item.hasNext()) {
            Map.Entry<Long, Object[]> kv = item.next();
            Long key = kv.getKey();
            int bt = (int) ((lastWindow - key)/windowSlide);
            if (bt >= 1 && bt <= windowSplit) {
                Object[] data = kv.getValue();
                for (int i = 0; i < data.length; i++) {
                    Object result = data[i];
                    if (result instanceof RoaringBitmap) {
                        ((RoaringBitmap) value[i]).or((RoaringBitmap) result);
                    } else if (result instanceof Roaring64NavigableMap) {
                        ((Roaring64NavigableMap) value[i]).or((Roaring64NavigableMap) result);
                    } else if (result instanceof Long) {
                        value[i] = ((Long) value[i]) + ((Long) result);
                    } else if (result instanceof Double) {
                        value[i] = ((Double) value[i]) + ((Double) result);
                    }
                }
            } else if (bt > windowSplit) {
                removeKey.add(key);
            }
        }

        for (int i = 0; i < removeKey.size(); i++) {
            globalWindow.remove(removeKey.get(i));
        }
    }

    @Override
    public Object getValue() throws Exception {
        Object[] data = new Object[value.length];
        for (int i = 0; i < data.length; i++) {
            Object result = value[i];
            if (result instanceof RoaringBitmap) {
                data[i] = ((RoaringBitmap) result).getLongCardinality();
            } else if (result instanceof Roaring64NavigableMap) {
                data[i] = ((Roaring64NavigableMap) result).getLongCardinality();
            } else {
                data[i] = result;
            }
        }
        return data;
    }
}
