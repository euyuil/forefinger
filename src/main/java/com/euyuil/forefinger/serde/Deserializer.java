package com.euyuil.forefinger.serde;

import com.euyuil.forefinger.meta.MetaData;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.24
 */
public interface Deserializer {

    void setMetaData(MetaData metaData);

    DataRow deserialize(String serializedDataRow);
}
