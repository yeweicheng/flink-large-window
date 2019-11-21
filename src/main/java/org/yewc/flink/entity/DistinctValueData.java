package org.yewc.flink.entity;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.yewc.flink.util.DateUtils;

import java.util.*;

public class DistinctValueData extends ValueData {

    public Roaring64NavigableMap value;
    public Map<Long, Roaring64NavigableMap> globalWindow;

    public DistinctValueData(Long windowUnix, Long windowSlide, int timeField, int theField) {
        this(windowUnix, windowSlide, 0L, timeField, theField);
    }

    public DistinctValueData(Long windowUnix, Long windowSlide, Long lateness, int timeField, int theField) {
        super(windowUnix, windowSlide, lateness, timeField, theField);
        this.value = new Roaring64NavigableMap();
        this.globalWindow = new HashMap<>(5);
    }

    @Override
    public void putElements(Iterable<Row> elements) throws Exception {

        Map<Long, Roaring64NavigableMap> smallWindow = new HashMap<>();
        for (Row row: elements) {
            long time = DateUtils.parse(row.getField(timeField));
            long start = TimeWindow.getWindowStartWithOffset(time, 0, windowSlide);
            if (!smallWindow.containsKey(start)) {
                smallWindow.put(start, new Roaring64NavigableMap());
            }
            smallWindow.get(start).add(Long.valueOf(row.getField(theField).toString().trim()).intValue());
        }

        Iterator<Long> item = smallWindow.keySet().iterator();
        while(item.hasNext()){
            Long key = item.next();
            if (globalWindow.containsKey(key)) {
                globalWindow.get(key).or(smallWindow.get(key));
            } else {
                globalWindow.put(key, smallWindow.get(key));
            }
        }

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
