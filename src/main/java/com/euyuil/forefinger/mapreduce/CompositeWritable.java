package com.euyuil.forefinger.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Reused between multiple Map operations.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.22
 */
public class CompositeWritable implements WritableComparable<CompositeWritable> {

    private List<SchemaItem> schemaItems;

    private Comparable[] objects;

    public void setSchemaItems(List<SchemaItem> schemaItems) {
        this.schemaItems = schemaItems;
        if (objects == null || objects.length < schemaItems.size())
            objects = new Comparable[schemaItems.size() * 2];
    }

    public void setObject(int index, Comparable object) {
        objects[index] = object;
    }

    public Comparable getObject(int index) {
        return objects[index];
    }

    @Override
    public int compareTo(CompositeWritable that) {

        int compareToResult = 0;
        int size = schemaItems.size();
        assert (size == that.schemaItems.size());

        for (int i = 0; i < size; ++i) {

            assert(this.schemaItems.get(i).isAscending() == that.schemaItems.get(i).isAscending());

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
                if (!this.schemaItems.get(i).isAscending())
                    compareToResult = -compareToResult;
                break;
            }
        }

        return compareToResult;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for (int i = 0; i < schemaItems.size(); ++i) {
            SchemaItem schemaItem = schemaItems.get(i);
            Class itemType = schemaItem.getType();
            if (itemType.equals(Integer.class))
                dataOutput.writeInt((Integer) objects[i]);
            else if (itemType.equals(Long.class))
                dataOutput.writeLong((Long) objects[i]);
            else if (itemType.equals(String.class))
                dataOutput.writeUTF((String) objects[i]);
            else if (itemType.equals(Double.class))
                dataOutput.writeDouble((Double) objects[i]);
            else if (itemType.equals(Float.class))
                dataOutput.writeFloat((Float) objects[i]);
            else
                assert(false);
            // TODO Other types.
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (int i = 0; i < schemaItems.size(); ++i) {
            SchemaItem schemaItem = schemaItems.get(i);
            Class itemType = schemaItem.getType();
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
                assert(false);
            // TODO Other types.
        }
    }

    public static class SchemaItem {

        private Class type;

        private boolean ascending;

        public SchemaItem(Class type, boolean ascending) {
            this.type = type;
            this.ascending = ascending;
        }

        public Class getType() {
            return type;
        }

        public boolean isAscending() {
            return ascending;
        }
    }
}
