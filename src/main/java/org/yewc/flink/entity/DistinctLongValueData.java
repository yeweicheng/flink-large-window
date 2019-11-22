package org.yewc.flink.entity;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.yewc.flink.util.DateUtils;

import java.util.*;

public class DistinctLongValueData extends ValueData {

    public Roaring64NavigableMap value;
    public Map<Long, Roaring64NavigableMap> globalWindow;

    public DistinctLongValueData(Long windowUnix, Long windowSlide, int timeField, int theField) {
        this(windowUnix, windowSlide, 0L, timeField, theField);
    }

    public DistinctLongValueData(Long windowUnix, Long windowSlide, Long lateness, int timeField, int theField) {
        super(windowUnix, windowSlide, lateness, timeField, theField);
        this.value = new Roaring64NavigableMap();
        this.globalWindow = new HashMap<>(5);
    }

    @Override
    public void putElements(Iterable<Row> elements) throws Exception {

        // 将每个row放入它当前数据时间的窗口
        Map<Long, Roaring64NavigableMap> smallWindow = new HashMap<>(8);
        Iterator<Row> eleIter = elements.iterator();
        long time;
        long start;
        Row row;
        while (eleIter.hasNext()) {
            row = eleIter.next();
            time = DateUtils.parse(row.getField(timeField));
            start = TimeWindow.getWindowStartWithOffset(time, 0, windowSlide);
            if (!smallWindow.containsKey(start)) {
                smallWindow.put(start, new Roaring64NavigableMap());
            }
            smallWindow.get(start).add((Long)row.getField(theField));
        }

        // 合并全局数据
        Iterator<Long> item = smallWindow.keySet().iterator();
        while(item.hasNext()){
            Long key = item.next();
            if (globalWindow.containsKey(key)) {
                globalWindow.get(key).or(smallWindow.get(key));
            } else {
                globalWindow.put(key, smallWindow.get(key));
            }
        }

        // 计算当前窗口
        value.clear();
        List<Long> removeKey = new ArrayList<>();
        item = globalWindow.keySet().iterator();
        while (item.hasNext()) {
            Long key = item.next();
            int bt = (int) ((lastWindow - key)/windowSlide);
            if (bt >= 1 && bt <= windowSplit) {
                value.or(globalWindow.get(key));
            } else if (bt > windowSplit) {
                removeKey.add(key);
            }
        }

        // 移除过期数据
        for (int i = 0; i < removeKey.size(); i++) {
            globalWindow.remove(removeKey.get(i));
        }

//        item = globalWindow.keySet().iterator();
//        while (item.hasNext()) {
//            System.out.print(globalWindow.get(item.next()).toString());
//        }
//        System.out.println(lastWindow);
    }

    @Override
    public Object getValue() throws Exception {
        return value.getLongCardinality();
    }
}
