package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.meta.view.OrderViewMetaData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Liu Yue
 * @version 0.0.2014.04.03
 */
public class CompositeKeyWritableEx extends CompositeKeyWritable {

    private static final char ASCENDING = 'a';
    private static final char DESCENDING = 'd';

    private char[] ordering;

    public CompositeKeyWritableEx() {
    }

    public CompositeKeyWritableEx(int size) {
        super(size);
    }

    public void setObjectOrderBy(int index, char order) {
        this.ordering[index] = order;
    }

    public void setObjectOrderBy(int index, OrderViewMetaData.OrderByItem.OrderType order) {
        if (order == OrderViewMetaData.OrderByItem.OrderType.ASCENDING)
            setObjectOrderBy(index, ASCENDING);
        else if (order == OrderViewMetaData.OrderByItem.OrderType.DESCENDING)
            setObjectOrderBy(index, DESCENDING);
    }

    public OrderViewMetaData.OrderByItem.OrderType getObjectOrderBy(int index) {
        char order = this.ordering[index];
        if (order == ASCENDING)
            return OrderViewMetaData.OrderByItem.OrderType.ASCENDING;
        else if (order == DESCENDING)
            return OrderViewMetaData.OrderByItem.OrderType.DESCENDING;
        return null;
    }

    /**
     * Call this before write, or use the corresponding constructor. But if you read, it's not necessary.
     * @param size the size of the value set.
     */
    @Override
    public void resize(int size) {
        super.resize(size);
        this.ordering = new char[size];
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

            if (compareToResult != 0) {
                if (this.ordering[i] == DESCENDING)
                    compareToResult = -compareToResult;
                break;
            }
        }

        return compareToResult;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(new String(ordering));
        super.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.ordering = dataInput.readUTF().toCharArray();
        super.readFields(dataInput);
    }
}
