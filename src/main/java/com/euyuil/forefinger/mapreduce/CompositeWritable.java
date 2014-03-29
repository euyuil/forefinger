package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.meta.OrderViewMetaData;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Reused between multiple Map operations.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.22
 */
public class CompositeWritable implements WritableComparable<CompositeWritable> {

    private static final char ASCENDING = 'A';
    private static final char DESCENDING = 'D';

    private char[] ordering;

    private Comparable[] objects;

    public void setObject(int index, Comparable object) {
        objects[index] = object;
    }

    public Comparable getObject(int index) {
        return objects[index];
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

    public CompositeWritable() {
    }

    public CompositeWritable(int size) {
        resize(size);
    }

    /**
     * Call this before write, or use the corresponding constructor. But if you read, it's not necessary.
     * @param size the size of the value set.
     */
    public void resize(int size) {
        this.objects = new Comparable[size];
        this.ordering = new char[size];
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(CompositeWritable that) {

        int compareToResult = 0;

        for (int i = 0; i < objects.length; ++i) {

            if (this.objects[i] == null) {
                if (that.objects[i] == null) {
                    compareToResult = 0; // this is null and that is null
                } else {
                    compareToResult = -1; // this is null but that is not null
                }
            } else if (that.objects[i] == null) {
                compareToResult = 1; // this is not null but that is null
            } else {
                compareToResult = this.objects[i].compareTo(that.objects[i]);
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
        for (Comparable object : objects) {
            Class itemType = object.getClass();
            dataOutput.writeChar(typeToChar(itemType));
            if (itemType.equals(Integer.class))
                dataOutput.writeInt((Integer) object);
            else if (itemType.equals(Long.class))
                dataOutput.writeLong((Long) object);
            else if (itemType.equals(String.class))
                dataOutput.writeUTF((String) object);
            else if (itemType.equals(Double.class))
                dataOutput.writeDouble((Double) object);
            else if (itemType.equals(Float.class))
                dataOutput.writeFloat((Float) object);
            else
                throw new IOException(String.format("Type unsupported: %s", itemType));
            // TODO Other types.
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.ordering = dataInput.readUTF().toCharArray();
        for (int i = 0; i < ordering.length; ++i) {
            Class itemType = charToType(ordering[i]);
            if (itemType.equals(Integer.class))
                objects[i] = dataInput.readInt();
            else if (itemType.equals(Long.class))
                objects[i] = dataInput.readLong();
            else if (itemType.equals(String.class))
                objects[i] = dataInput.readUTF();
            else if (itemType.equals(Double.class))
                objects[i] = dataInput.readDouble();
            else if (itemType.equals(Float.class))
                objects[i] = dataInput.readFloat();
            else
                throw new IOException(String.format("Type unsupported: %s", itemType));
            // TODO Other types.
        }
    }

    private static Class charToType(char c) {
        switch (c) {
            case 'i': return Integer.class;
            case 'l': return Long.class;
            case 't': return String.class;
            case 'd': return Double.class;
            case 'f': return Float.class;
        }
        return null;
    }

    private static char typeToChar(Class type) {
        if (type.equals(Integer.class)) return 'i';
        if (type.equals(Long.class)) return 'l';
        if (type.equals(String.class)) return 't';
        if (type.equals(Double.class)) return 'd';
        if (type.equals(Float.class)) return 'f';
        return ' ';
    }
}
