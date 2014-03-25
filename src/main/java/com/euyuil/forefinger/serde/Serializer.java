package com.euyuil.forefinger.serde;

import com.euyuil.forefinger.meta.MetaData;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.24
 */
public interface Serializer {

    void setMetaData(MetaData metaData);

    String serialize(DataRow dataRow);
}
