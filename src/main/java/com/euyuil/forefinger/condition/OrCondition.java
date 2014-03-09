package com.euyuil.forefinger.condition;

import com.euyuil.forefinger.meta.MetaDataColumn;
import com.euyuil.forefinger.DataRow;

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
