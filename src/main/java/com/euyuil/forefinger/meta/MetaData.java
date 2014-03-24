package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.serde.DataSerDe;
import com.euyuil.forefinger.serde.Deserializer;
import com.euyuil.forefinger.serde.Serializer;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public abstract class MetaData {

    /**
     * Constructs a MetaData object specifying MetaDataSet object.
     * @param metaDataSet the MetaDataSet object.
     */
    public MetaData(MetaDataSet metaDataSet) {
        setMetaDataSet(metaDataSet);
    }

    /**
     * The MetaDataSet object that generates this object.
     */
    @XStreamOmitField
    private MetaDataSet metaDataSet;

    public MetaDataSet getMetaDataSet() {
        return metaDataSet;
    }

    public void setMetaDataSet(MetaDataSet metaDataSet) {
        this.metaDataSet = metaDataSet;
    }

    /**
     * The name of the MetaData object.
     */
    @XStreamAsAttribute
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * The columns of the MetaData object.
     */
    @XStreamImplicit
    public ArrayList<MetaDataColumn> metaDataColumns;

    public List<MetaDataColumn> getMetaDataColumns() {
        return Collections.unmodifiableList(metaDataColumns);
    }

    public void setMetaDataColumns(ArrayList<MetaDataColumn> metaDataColumns) {
        this.metaDataColumns = metaDataColumns;
        // TODO Save meta data or something.
        // TODO Maybe copying is better.
    }

    public int getMetaDataColumnIndex(String columnName) {
        for (int i = 0; i < metaDataColumns.size(); i++) {
            MetaDataColumn metaDataColumn = metaDataColumns.get(i);
            if (metaDataColumn.getName().equalsIgnoreCase(columnName))
                return i;
        }
        return -1;
    }

    public MetaDataColumn getMetaDataColumn(String columnName) {
        int ret = getMetaDataColumnIndex(columnName);
        if (ret < 0)
            return null;
        return metaDataColumns.get(ret);
    }

    /**
     * @return the serializer of the data.
     */
    public abstract Serializer getSerializer();

    /**
     * @return the deserializer of the data.
     */
    public abstract Deserializer getDeserializer();

    @SuppressWarnings("unchecked")
    public <T extends MetaDataColumn> T getMetaDataColumn(String columnName, Class<T> clazz) {
        MetaDataColumn metaDataColumn = getMetaDataColumn(columnName);
        if (metaDataColumn != null && metaDataColumn.getResultType().equals(clazz))
            return (T) metaDataColumn;
        return null;
    }

    /**
     * Sub-classes should override this method to return its XML SerDe.
     * Factory-method pattern.
     * @return the XML SerDe.
     */
    protected abstract XStream getStaticXmlSerDe();

    public String toXml() {
        return getStaticXmlSerDe().toXML(this);
    }

    public void toXmlFile(File file) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(file);
        toXmlStream(outputStream);
    }

    public void toXmlStream(OutputStream outputStream) throws IOException {
        getStaticXmlSerDe().toXML(this, outputStream);
        outputStream.flush();
        outputStream.close();
    }
}
