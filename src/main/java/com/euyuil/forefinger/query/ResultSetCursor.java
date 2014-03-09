package com.euyuil.forefinger.query;

import java.util.Iterator;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.07
 */
public class ResultSetCursor implements Iterator<DataRow> {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public DataRow next() {
        return null;
    }

    public boolean hasPrevious() {
        return false;
    }

    public DataRow previous() {
        return null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException(
                "Cannot remove items in result set");
    }
}
