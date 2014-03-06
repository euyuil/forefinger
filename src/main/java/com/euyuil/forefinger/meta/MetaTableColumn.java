package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * TODO Nullable and default.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
@XStreamAlias("column")
public class MetaTableColumn extends MetaDataColumn {

    public MetaTableColumn() {
    }

    public MetaTableColumn(String name, Class type) {
        super(name, type);
    }
}
