package com.euyuil.forefinger.serde;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.07
 */
public abstract class DataRow {

    public abstract Object get(int columnIndex);

    @SuppressWarnings("unchecked")
    public <T> T get(int columnIndex, Class<T> clazz) {
        Object value = (T) get(columnIndex);
        if (value.getClass().equals(clazz))
            return (T) value;
        return null;
    }

    public abstract int size();
}
