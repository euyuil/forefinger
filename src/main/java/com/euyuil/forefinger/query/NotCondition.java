package com.euyuil.forefinger.query;

import com.euyuil.forefinger.meta.MetaDataColumn;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.08
 */
public class NotCondition extends Condition {

    public NotCondition(Object a) {

    }

    public NotCondition(MetaDataColumn a) {

    }

    @Override
    public boolean fulfilled(DataRow dataRow) {
        return false;
    }
}
