package com.euyuil.forefinger.meta.condition;

import com.euyuil.forefinger.meta.MetaDataColumn;
import com.euyuil.forefinger.serde.DataRow;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public class EqualCondition extends Condition {

    public EqualCondition(Object a, Object b) {

    }

    public EqualCondition(MetaDataColumn a, Object b) {

    }

    public EqualCondition(Object a, MetaDataColumn b) {

    }

    public EqualCondition(MetaDataColumn a, MetaDataColumn b) {

    }

    @Override
    public boolean fulfilled(DataRow dataRow) {
        return false;
    }
}
