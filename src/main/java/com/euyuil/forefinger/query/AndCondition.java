package com.euyuil.forefinger.query;

import com.euyuil.forefinger.meta.MetaDataColumn;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public class AndCondition extends Condition {

    public AndCondition(Object a, Object b) {

    }

    public AndCondition(MetaDataColumn a, Object b) {

    }

    public AndCondition(Object a, MetaDataColumn b) {

    }

    public AndCondition(MetaDataColumn a, MetaDataColumn b) {

    }

    @Override
    public boolean fulfilled(DataRow dataRow) {
        return false;
    }
}
