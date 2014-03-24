package com.euyuil.forefinger.serde;

import java.util.ArrayList;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.24
 */
public class ArrayDataRow extends DataRow {

    public ArrayDataRow(int count) {
        values = new Object[count];
    }

    private Object[] values;

    @Override
    public Object get(int columnIndex) {
        return values[columnIndex];
    }

    public void set(int columnIndex, Object object) {
        this.values[columnIndex] = object;
    }

    @Override
    public int size() {
        return values.length;
    }
}
