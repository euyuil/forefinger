package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
@XStreamAlias("column")
public class ViewColumn extends DataColumn {

    public ViewColumn() {
    }

    public ViewColumn(String name, Class type) {
        super(name, type);
    }
}
