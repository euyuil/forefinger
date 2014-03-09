package com.euyuil.forefinger.query;

import com.euyuil.forefinger.meta.MetaDataColumn;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public class OrCondition extends Condition {

    private Condition left;
    private Condition right;

    public OrCondition(Object a, Object b) {

    }

    public OrCondition(MetaDataColumn a, Object b) {

    }

    public OrCondition(Object a, MetaDataColumn b) {

    }

    public OrCondition(MetaDataColumn a, MetaDataColumn b) {

    }

    @Override
    public boolean fulfilled(DataRow dataRow) {
        return false;
    }
}
