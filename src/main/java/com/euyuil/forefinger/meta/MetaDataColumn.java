package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.Expression;
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

    /**
     * Sub-classes should override this method to return the resulting type of the column.
     * @return the resulting type of the column.
     */
    @Override
    public abstract Class getResultType();
}
