package org.yewc.flink.watermark;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;
import org.yewc.flink.util.DateUtils;

public class EventWatermark implements AssignerWithPunctuatedWatermarks {

    private int field;

    public EventWatermark(int field) {
        this.field = field;
    }

    @Override
    public Watermark checkAndGetNextWatermark(Object lastElement, long extractedTimestamp) {
        return new Watermark(extractedTimestamp);
    }

    @Override
    public long extractTimestamp(Object element, long previousElementTimestamp) {
        Row data = (Row) element;
        return DateUtils.parse(data.getField(this.field));
    }
}
