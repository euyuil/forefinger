package com.euyuil.forefinger.serde;

import com.euyuil.forefinger.DataRow;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.24
 */
public interface Serializer {

    String serialize(DataRow dataRow);
}
