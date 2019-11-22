package org.yewc.flink.entity;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.roaringbitmap.RoaringBitmap;
import org.yewc.flink.util.DateUtils;

import java.util.*;

public class CountValueData extends ValueData {

    private Long value;
    private Map<Long, Long> globalWindow;

    public CountValueData(Long windowUnix, Long windowSlide, int timeField, int theField) {
        this(windowUnix, windowSlide, 0L, timeField, theField);
    }

    public CountValueData(Long windowUnix, Long windowSlide, Long lateness, int timeField, int theField) {
        super(windowUnix, windowSlide, lateness, timeField, theField);
        this.value = 0L;
        this.globalWindow = new HashMap<>();
    }

    @Override
    public void putElements(Iterable<Row> elements) throws Exception {
        Map<Long, Long> smallWindow = new HashMap<>(8);
        Iterator<Row> eleIter = elements.iterator();
        long time;
        long start;
        Row row;
        while (eleIter.hasNext()) {
            row = eleIter.next();
            time = DateUtils.parse(row.getField(timeField));
            start = TimeWindow.getWindowStartWithOffset(time, 0, windowSlide);
            if (!smallWindow.containsKey(start)) {
                smallWindow.put(start, 0L);
            }

            Object fieldData = row.getField(theField);
            if (fieldData != null) {
                smallWindow.put(start, smallWindow.get(start) + 1);
            }
        }

        Iterator<Long> item = smallWindow.keySet().iterator();
        while(item.hasNext()){
            Long key = item.next();
            if (globalWindow.containsKey(key)) {
                globalWindow.put(key, globalWindow.get(key) + smallWindow.get(key));
            } else {
                globalWindow.put(key, smallWindow.get(key));
            }
        }

        value = 0L;

        List<Long> removeKey = new ArrayList<>();
        item = globalWindow.keySet().iterator();
        while (item.hasNext()) {
            Long key = item.next();
            int bt = (int) ((lastWindow - key)/windowSlide);
            if (bt >= 1 && bt <= windowSplit) {
                value += globalWindow.get(key);
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
        return value;
    }
}
