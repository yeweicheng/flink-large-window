package org.yewc.flink.watermark;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;
import org.yewc.flink.util.DateUtils;

public class TheWatermark implements AssignerWithPunctuatedWatermarks {

    private int field;
    private boolean event = false;

    public TheWatermark(int field, TimeCharacteristic tc) {
        this.field = field;
        if (TimeCharacteristic.EventTime.equals(tc)) {
            event = true;
        }
    }

    @Override
    public Watermark checkAndGetNextWatermark(Object lastElement, long extractedTimestamp) {
        return new Watermark(extractedTimestamp);
    }

    @Override
    public long extractTimestamp(Object element, long previousElementTimestamp) {
        if (!event) {
            return System.currentTimeMillis();
        }

        Row data = (Row) element;
        long thisTime = DateUtils.parse(data.getField(this.field));
        return thisTime < previousElementTimestamp ? thisTime : previousElementTimestamp;
    }
}
