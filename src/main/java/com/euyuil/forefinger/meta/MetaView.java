package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.annotations.XStreamImplicit;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public class MetaView extends MetaData {

    @XStreamImplicit(itemFieldName = "column")
    private MetaViewColumn[] columns;

    public MetaViewColumn[] getViewColumns() {
        return columns;
    }

    public void setViewColumns(MetaViewColumn[] columns) {
        this.columns = columns;
    }

    @Override
    public MetaDataColumn[] getDataColumns() {
        return columns;
    }
}
