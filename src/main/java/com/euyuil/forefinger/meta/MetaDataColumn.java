package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public abstract class MetaDataColumn implements Expression {

    /**
     * Constructs a MetaDataColumn object specifying MetaData object.
     * @param metaData the MetaData object.
     */
    public MetaDataColumn(MetaData metaData) {
        this.metaData = metaData;
    }

    /**
     * Constructs a MetaDataColumn object specifying MetaData object and column name.
     * @param metaData the MetaData object.
     * @param name the column name.
     */
    public MetaDataColumn(MetaData metaData, String name) {
        this.metaData = metaData;
        this.name = name;
    }

    /**
     * The name of the column.
     */
    @XStreamAsAttribute
    protected String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * The reference to the MetaData object.
     */
    @XStreamOmitField
    protected MetaData metaData;

    public MetaData getMetaData() {
        return metaData;
    }

    protected void setMetaData(MetaData metaData) {
        this.metaData = metaData;
    }

    /**
     * Gets the MetaDataSet object that creates the MetaData object that creates this object.
     */
    public MetaDataSet getMetaDataSet() {
        return getMetaData().getMetaDataSet();
    }
}
