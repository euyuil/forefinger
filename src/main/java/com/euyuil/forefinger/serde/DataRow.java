package com.euyuil.forefinger.serde;

import java.util.Iterator;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.07
 */
public abstract class DataRow implements Iterable<Object> {

    public abstract Object get(int columnIndex);

    @SuppressWarnings("unchecked")
    public <T> T get(int columnIndex, Class<T> clazz) {
        Object value = (T) get(columnIndex);
        if (value.getClass().equals(clazz))
            return (T) value;
        return null;
    }

    public abstract int size();

    @Override
    public Iterator<Object> iterator() {
        return new DataRowIterator();
    }

    public class DataRowIterator implements Iterator<Object> {

        private int index = 0;

        @Override
        public boolean hasNext() {
            return index < DataRow.this.size();
        }

        @Override
        public Object next() {
            return DataRow.this.get(index++);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
