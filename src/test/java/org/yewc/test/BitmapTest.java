package org.yewc.test;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

public class BitmapTest {

    public static void main(String[] args) throws Exception {
        long s = System.currentTimeMillis();
        for (int j = 0; j < 10000; j++) {
            Roaring64NavigableMap all = new Roaring64NavigableMap();
            for (int i = 0; i < 100; i++) {
                Roaring64NavigableMap temp = new Roaring64NavigableMap();
                temp.add(i + 100000000);
                all.or(temp);
//                all.add(i + 100000000);
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
