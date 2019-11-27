package org.yewc.test;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class BitmapTest {

    public static void main(String[] args) throws Exception {
        System.out.println(TimeWindow.getWindowStartWithOffset(1574761559986L, 0, 60000));
    }

    public static void main1(String[] args) throws Exception {
        RoaringBitmap o = new RoaringBitmap();
        o.add(1000000000, 1000002000);
        System.out.println(o.getLongSizeInBytes());

        Roaring64NavigableMap a = new Roaring64NavigableMap();
        a.add(1000000000, 1000002000);
        System.out.println(a.getLongSizeInBytes());

        Roaring64NavigableMap b = new Roaring64NavigableMap();
        b.add(1000000000, 1000001000);
        System.out.println(b.getLongSizeInBytes());

        Roaring64NavigableMap c = new Roaring64NavigableMap();
        c.add(1000001001, 1000002000);
        System.out.println(c.getLongSizeInBytes());
    }

}
