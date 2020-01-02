package org.yewc.flink.entity;

import org.apache.flink.types.Row;

/**
 * use org.yewc.flink.entity.GlobalValueData
 */
@Deprecated
public abstract class ValueData {

    public Long lastWindow;
    public Long windowUnix;
    public Long windowSlide;
    public Long lateness;
    public int windowSplit;
    public int timeField;
    public int theField;

    public ValueData(Long windowUnix, Long windowSlide, int timeField, int theField) {
        this(windowUnix, windowSlide, 0L, timeField, theField);
    }

    public ValueData(Long windowUnix, Long windowSlide, Long lateness, int timeField, int theField) {
        this.windowUnix = windowUnix;
        this.windowSlide = windowSlide;
        this.lateness = lateness;
        this.timeField = timeField;
        this.theField = theField;
        this.windowSplit = new Long(windowUnix/windowSlide).intValue();
    }

    public abstract void putElements(Iterable<Row> elements) throws Exception;

    public abstract Object getValue() throws Exception;

}
