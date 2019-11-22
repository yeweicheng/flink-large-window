package org.yewc.test;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class BitmapTest {

    public static void main(String[] args) throws Exception {
        long s = System.currentTimeMillis();
        for (int j = 0; j < 10000; j++) {
//            Roaring64NavigableMap all = new Roaring64NavigableMap();
//            List<Roaring64NavigableMap> children = new ArrayList<>();
//            for (int i = 0; i < 100; i++) {
//                Roaring64NavigableMap current = new Roaring64NavigableMap();
//                current.add(i + 100000000);
//                children.add(current);
//                all.or(current);
////                all.add(i + 100000000);
//            }
            Set<Integer> all = new HashSet<>();
            for (int i = 0; i < 100; i++) {
                Set<Integer> dataSet = new HashSet<>();
                dataSet.add(i);
                all.addAll(dataSet);
            }
        }
        long e = System.currentTimeMillis();
        System.out.println(e - s);
//        System.out.println(all.getLongSizeInBytes());
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
