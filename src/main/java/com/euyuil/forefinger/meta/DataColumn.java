package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.query.EqualCondition;

import java.lang.reflect.Type;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public abstract class DataColumn {

    public abstract String getName();

    public abstract Type getType();

    public EqualCondition isEqualTo(Object value) {
        return new EqualCondition(this, value);
    }

    public EqualCondition isEqualTo(DataColumn column) {
        return new EqualCondition(this, column);
    }
}
