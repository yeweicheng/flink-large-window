package org.yewc.flink.entity;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.roaringbitmap.RoaringBitmap;
import org.yewc.flink.util.DateUtils;

import java.util.*;

/**
 * use org.yewc.flink.entity.GlobalValueData
 */
@Deprecated
public class DistinctIntValueData extends ValueData {

    public RoaringBitmap value;
    public Map<Long, RoaringBitmap> globalWindow;

    public DistinctIntValueData(Long windowUnix, Long windowSlide, int timeField, int theField) {
        this(windowUnix, windowSlide, 0L, timeField, theField);
    }

    public DistinctIntValueData(Long windowUnix, Long windowSlide, Long lateness, int timeField, int theField) {
        super(windowUnix, windowSlide, lateness, timeField, theField);
        this.value = new RoaringBitmap();
        this.globalWindow = new HashMap<>(5);
    }

    @Override
    public void putElements(Iterable<Row> elements) throws Exception {

        Map<Long, RoaringBitmap> smallWindow = new HashMap<>(8);
        Iterator<Row> eleIter = elements.iterator();
        long time;
        long start;
        Row row;
        while (eleIter.hasNext()) {
            row = eleIter.next();
            time = DateUtils.parse(row.getField(timeField));
            start = TimeWindow.getWindowStartWithOffset(time, 0, windowSlide);
            if (!smallWindow.containsKey(start)) {
                smallWindow.put(start, new RoaringBitmap());
            }
            smallWindow.get(start).add((Integer) row.getField(theField));
        }

        Iterator<Map.Entry<Long, RoaringBitmap>> item = smallWindow.entrySet().iterator();
        while(item.hasNext()){
            Map.Entry<Long, RoaringBitmap> kv = item.next();
            Long key = kv.getKey();
            RoaringBitmap data = kv.getValue();
            if (globalWindow.containsKey(key)) {
                globalWindow.get(key).or(data);
            } else {
                globalWindow.put(key, data);
            }
        }

        value.clear();

        List<Long> removeKey = new ArrayList<>();
        item = globalWindow.entrySet().iterator();
        while (item.hasNext()) {
            Map.Entry<Long, RoaringBitmap> kv = item.next();
            Long key = kv.getKey();
            int bt = (int) ((lastWindow - key)/windowSlide);
            if (bt >= 1 && bt <= windowSplit) {
                value.or(kv.getValue());
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
