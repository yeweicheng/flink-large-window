package org.yewc.flink.entity;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.yewc.flink.util.DateUtils;

import java.util.*;

public class DistinctObjectValueData extends ValueData {

    public Set<Object> value;
    public Map<Long, Set<Object>> globalWindow;

    public DistinctObjectValueData(Long windowUnix, Long windowSlide, int timeField, int theField) {
        this(windowUnix, windowSlide, 0L, timeField, theField);
    }

    public DistinctObjectValueData(Long windowUnix, Long windowSlide, Long lateness, int timeField, int theField) {
        super(windowUnix, windowSlide, lateness, timeField, theField);
        this.value = new HashSet<>();
        this.globalWindow = new HashMap<>(5);
    }

    @Override
    public void putElements(Iterable<Row> elements) throws Exception {

        Map<Long, Set<Object>> smallWindow = new HashMap<>(8);
        Iterator<Row> eleIter = elements.iterator();
        long time;
        long start;
        Row row;
        while (eleIter.hasNext()) {
            row = eleIter.next();
            time = DateUtils.parse(row.getField(timeField));
            start = TimeWindow.getWindowStartWithOffset(time, 0, windowSlide);
            if (!smallWindow.containsKey(start)) {
                smallWindow.put(start, new HashSet<>());
            }
            smallWindow.get(start).add(row.getField(theField));
        }

        Iterator<Map.Entry<Long, Set<Object>>> item = smallWindow.entrySet().iterator();
        while(item.hasNext()){
            Map.Entry<Long, Set<Object>> kv = item.next();
            Long key = kv.getKey();
            Set<Object> data = kv.getValue();
            if (globalWindow.containsKey(key)) {
                globalWindow.get(key).addAll(data);
            } else {
                globalWindow.put(key, data);
            }
        }

        value.clear();

        List<Long> removeKey = new ArrayList<>();
        item = globalWindow.entrySet().iterator();
        while (item.hasNext()) {
            Map.Entry<Long, Set<Object>> kv = item.next();
            Long key = kv.getKey();
            int bt = (int) ((lastWindow - key)/windowSlide);
            if (bt >= 1 && bt <= windowSplit) {
                value.addAll(kv.getValue());
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
        return new Long(value.size());
    }
}
