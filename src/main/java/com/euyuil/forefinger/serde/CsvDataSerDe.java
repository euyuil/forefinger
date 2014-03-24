package com.euyuil.forefinger.serde;

import com.euyuil.forefinger.DataRow;

/**
 * Not only comma separated values. It's characters separated values.
 * The default SerDe for views.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public class CsvDataSerDe implements DataSerDe {

    @Override
    public DataRow deserialize(String serializedDataRow) {
        return null;
    }

    @Override
    public String serialize(DataRow dataRow) {
        return null;
    }
}
