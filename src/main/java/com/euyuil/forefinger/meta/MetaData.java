package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.annotations.XStreamAsAttribute;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public abstract class MetaData {

    /**
     * The name of the table.
     */
    @XStreamAsAttribute
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public abstract MetaDataColumn[] getDataColumns();
}
