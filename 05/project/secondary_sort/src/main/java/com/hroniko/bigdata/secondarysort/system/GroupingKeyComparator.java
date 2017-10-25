package com.hroniko.bigdata.secondarysort.system;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// Класс-компаратор для группировки
public class GroupingKeyComparator extends WritableComparator {

    public GroupingKeyComparator() {
        super(ComparedKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComparedKey key1 = (ComparedKey) a;
        ComparedKey key2 = (ComparedKey) b;

        return key1.compareTo(key2);
    }
}