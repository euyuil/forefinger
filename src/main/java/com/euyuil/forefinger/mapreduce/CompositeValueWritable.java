package com.euyuil.forefinger.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Liu Yue
 * @version 0.0.2014.04.03
 */
public class CompositeValueWritable implements Writable {

    protected Object[] objects;

    public void setObject(int index, Object object) {
        objects[index] = object;
    }

    public Object getObject(int index) {
        return objects[index];
    }

    public CompositeValueWritable() {
    }

    public CompositeValueWritable(int size) {
        resize(size);
    }

    /**
     * Call this before write, or use the corresponding constructor. But if you read, it's not necessary.
     * @param size the size of the value set.
     */
    public void resize(int size) {
        this.objects = new Object[size];
    }

    public int size() {
        return this.objects.length;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(objects.length);

        for (Object object : objects) {

            Class itemType = object.getClass();
            dataOutput.writeChar(typeToChar(itemType));

            if (itemType.equals(Integer.class)) dataOutput.writeInt((Integer) object);
            else if (itemType.equals(Long.class)) dataOutput.writeLong((Long) object);
            else if (itemType.equals(String.class)) dataOutput.writeUTF((String) object);
            else if (itemType.equals(Double.class)) dataOutput.writeDouble((Double) object);
            else if (itemType.equals(Float.class)) dataOutput.writeFloat((Float) object);
            else throw new IOException(String.format("Type unsupported: %s", itemType));
            // TODO Other types.
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        int length = dataInput.readInt();

        for (int i = 0; i < length; ++i) {

            Class itemType = charToType(dataInput.readChar());

            if (itemType.equals(Integer.class)) setObject(i, dataInput.readInt());
            else if (itemType.equals(Long.class)) setObject(i, dataInput.readLong());
            else if (itemType.equals(String.class)) setObject(i, dataInput.readUTF());
            else if (itemType.equals(Double.class)) setObject(i, dataInput.readDouble());
            else if (itemType.equals(Float.class)) setObject(i, dataInput.readFloat());
            else throw new IOException(String.format("Type unsupported: %s", itemType));
            // TODO Other types.
        }
    }

    protected static Class charToType(char c) {
        switch (c) {
            case 'i': return Integer.class;
            case 'l': return Long.class;
            case 't': return String.class;
            case 'd': return Double.class;
            case 'f': return Float.class;
        }
        return null;
    }

    protected static char typeToChar(Class type) {
        if (type.equals(Integer.class)) return 'i';
        if (type.equals(Long.class)) return 'l';
        if (type.equals(String.class)) return 't';
        if (type.equals(Double.class)) return 'd';
        if (type.equals(Float.class)) return 'f';
        return ' ';
    }
}
