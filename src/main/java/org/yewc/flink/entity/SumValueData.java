package org.yewc.flink.entity;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.yewc.flink.util.DateUtils;

import java.util.*;

public class SumValueData extends ValueData {

    private Double value;
    private Map<Long, Double> globalWindow;

    public SumValueData(Long windowUnix, Long windowSlide, int timeField, int theField) {
        this(windowUnix, windowSlide, 0L, timeField, theField);
    }

    public SumValueData(Long windowUnix, Long windowSlide, Long lateness, int timeField, int theField) {
        super(windowUnix, windowSlide, lateness, timeField, theField);
        this.value = 0.0;
        this.globalWindow = new HashMap<>();
    }

    @Override
    public void putElements(Iterable<Row> elements) throws Exception {
        Map<Long, Double> smallWindow = new HashMap<>(8);
        Iterator<Row> eleIter = elements.iterator();
        long time;
        long start;
        Row row;
        while (eleIter.hasNext()) {
            row = eleIter.next();
            time = DateUtils.parse(row.getField(timeField));
            start = TimeWindow.getWindowStartWithOffset(time, 0, windowSlide);
            if (!smallWindow.containsKey(start)) {
                smallWindow.put(start, 0.0);
            }

            Object fieldData = row.getField(theField);
            if (fieldData != null) {
                smallWindow.put(start, smallWindow.get(start) + Double.valueOf(fieldData.toString()));
            }
        }

        Iterator<Map.Entry<Long, Double>> item = smallWindow.entrySet().iterator();
        while(item.hasNext()){
            Map.Entry<Long, Double> kv = item.next();
            Long key = kv.getKey();
            Double data = kv.getValue();
            if (globalWindow.containsKey(key)) {
                globalWindow.put(key, globalWindow.get(key) + data);
            } else {
                globalWindow.put(key, data);
            }
        }

        value = 0.0;

        List<Long> removeKey = new ArrayList<>();
        item = globalWindow.entrySet().iterator();
        while (item.hasNext()) {
            Map.Entry<Long, Double> kv = item.next();
            Long key = kv.getKey();
            int bt = (int) ((lastWindow - key)/windowSlide);
            if (bt >= 1 && bt <= windowSplit) {
                value += kv.getValue();
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
