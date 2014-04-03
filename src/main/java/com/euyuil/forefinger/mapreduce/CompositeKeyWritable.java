package com.euyuil.forefinger.mapreduce;

import org.apache.hadoop.io.WritableComparable;

/**
 * Reused between multiple Map operations.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.22
 */
public class CompositeKeyWritable extends CompositeValueWritable
        implements WritableComparable<CompositeKeyWritable> {

    public Comparable getComparableObject(int index) {
        return (Comparable) getObject(index);
    }

    public CompositeKeyWritable() {
    }

    public CompositeKeyWritable(int size) {
        resize(size);
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(CompositeKeyWritable that) {

        int compareToResult = 0;

        for (int i = 0; i < objects.length; ++i) {

            if (this.getObject(i) == null) {
                if (that.getObject(i) == null) {
                    compareToResult = 0; // this is null and that is null
                } else {
                    compareToResult = -1; // this is null but that is not null
                }
            } else if (that.getObject(i) == null) {
                compareToResult = 1; // this is not null but that is null
            } else {
                compareToResult = this.getComparableObject(i).compareTo(that.getComparableObject(i));
            }

        }

        return compareToResult;
    }
}
