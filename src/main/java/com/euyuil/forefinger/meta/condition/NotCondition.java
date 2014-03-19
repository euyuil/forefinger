package com.euyuil.forefinger.meta.condition;

import com.euyuil.forefinger.meta.MetaDataColumn;
import com.euyuil.forefinger.DataRow;

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
