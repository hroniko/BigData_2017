package com.hroniko.bigdata.secondarysort.system;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// Класс-компаратор дяъля сортировки
public class CompositeKeyComparator extends WritableComparator {

    public CompositeKeyComparator() {
        super(ComparedKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        ComparedKey ckey1 = (ComparedKey) a;
        ComparedKey ckey2 = (ComparedKey) b;

        return ckey1.compareTo(ckey2);
    }
}