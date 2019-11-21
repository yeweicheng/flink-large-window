package org.yewc.flink.util;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class DateUtils {

    private final static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final static ZoneOffset zo = ZoneOffset.ofHours(8);

    /***
     * second
     * @return
     */
    public static String format(Long second) {
        return dtf.format(LocalDateTime.ofEpochSecond(second, 0, zo));
    }

    /***
     * second
     * @return
     */
    public static Long parse(Object data) {
        if (data instanceof Timestamp) {
            return ((Timestamp) data).getTime();
        }

        if (data instanceof String || data instanceof Long) {
            String time = data.toString();
            if (time.length() == 10) {
                return Long.valueOf(time)*1000;
            }

            if (time.length() == 13) {
                return Long.valueOf(time);
            }

            time = time.substring(0, 19);
            return LocalDateTime.parse(time, dtf).toEpochSecond(zo)*1000;
        }
        return null;
    }
}
