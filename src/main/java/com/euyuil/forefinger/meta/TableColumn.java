package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * TODO Nullable and default.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
@XStreamAlias("column")
public class TableColumn extends DataColumn {

    public TableColumn() {
    }

    public TableColumn(String name, Class type) {
        super(name, type);
    }
}
