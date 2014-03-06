package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.annotations.XStreamAsAttribute;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public abstract class Data {

    /**
     * The name of the table.
     */
    @XStreamAsAttribute
    private String name;

    /**
     * The schema of the table.
     */
    private TableSchema schema;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public void setSchema(TableSchema schema) {
        this.schema = schema;
    }
}
