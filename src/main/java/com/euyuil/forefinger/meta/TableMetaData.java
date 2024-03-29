package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.serde.CsvDataSerDe;
import com.euyuil.forefinger.serde.DataSerDe;
import com.euyuil.forefinger.serde.Deserializer;
import com.euyuil.forefinger.serde.Serializer;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The class for a data table.
 * It's size could be really large,
 * and it might be managed by clients.
 * So it will be stored in HDFS.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
@XStreamAlias("table")
public class TableMetaData extends MetaData {

    /**
     * The serializer and deserializer of the table.
     */
    @XStreamAlias("serde")
    private DataSerDe serDe;

    public DataSerDe getSerDe() {
        if (serDe == null) {
            serDe = new CsvDataSerDe();
            serDe.setMetaData(this);
        }
        return serDe;
    }

    public void setSerDe(DataSerDe serDe) {
        this.serDe = serDe;
    }

    @Override
    public Serializer getSerializer() {
        return getSerDe();
    }

    @Override
    public Deserializer getDeserializer() {
        return getSerDe();
    }

    /**
     * The indices of the table data.
     */
    @XStreamImplicit(itemFieldName = "index")
    private ArrayList<TableMetaDataIndex> indices;

    public List<TableMetaDataIndex> getIndices() {
        return Collections.unmodifiableList(indices);
    }

    public void setIndices(ArrayList<TableMetaDataIndex> indices) {
        this.indices = indices;
        // TODO Do something to save meta data and generate indices.
    }

    /**
     * The sources of the table data sets.
     */
    @XStreamImplicit(itemFieldName = "source")
    private ArrayList<String> sources;

    public List<String> getSources() {
        return Collections.unmodifiableList(sources);
    }

    public void setSources(ArrayList<String> sources) {
        this.sources = sources;
        // TODO Do something to save meta data.
    }

    /**
     * Constructs a TableMetaData object specifying MetaDataSet object.
     * @param metaDataSet the MetaDataSet object.
     */
    public TableMetaData(MetaDataSet metaDataSet) {
        super(metaDataSet);
    }

    /**
     * @return the XML SerDe.
     */
    @Override
    protected XStream getStaticXmlSerDe() {
        return getXmlSerDe();
    }

    static XStream xmlSerDe;

    static XStream getXmlSerDe() {
        if (xmlSerDe == null) {
            xmlSerDe = new XStream();
            xmlSerDe.processAnnotations(new Class[]{
                    MetaData.class,
                    MetaDataColumn.class,
                    TableMetaData.class,
                    TableMetaDataColumn.class,
                    TableMetaDataIndex.class,
            });
        }
        return xmlSerDe;
    }

    public static TableMetaData fromXml(String xml) {
        return (TableMetaData) xmlSerDe.fromXML(xml);
    }

    public static TableMetaData fromXmlFile(File xmlFile) {
        return (TableMetaData) xmlSerDe.fromXML(xmlFile);
    }

    public static TableMetaData fromXmlStream(InputStream stream) {
        return (TableMetaData) xmlSerDe.fromXML(stream);
    }
}
