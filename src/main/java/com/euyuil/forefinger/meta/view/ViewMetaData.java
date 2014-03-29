package com.euyuil.forefinger.meta.view;

import com.euyuil.forefinger.meta.MetaData;
import com.euyuil.forefinger.meta.MetaDataColumn;
import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.meta.condition.Condition;
import com.euyuil.forefinger.serde.CsvDataSerDe;
import com.euyuil.forefinger.serde.Deserializer;
import com.euyuil.forefinger.serde.Serializer;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

import java.io.File;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
@XStreamAlias("view")
public class ViewMetaData extends MetaData {

    /**
     * Constructs a ViewMetaData object specifying MetaDataSet object.
     * @param metaDataSet the MetaDataSet object.
     */
    public ViewMetaData(MetaDataSet metaDataSet) {
        super(metaDataSet);
    }

    /**
     * Condition filters.
     */
    @XStreamAlias("where")
    private Condition condition;

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
        // TODO Save.
    }

    public boolean isTemporary() {
        return false; // TODO
    }

    @XStreamOmitField
    private CsvDataSerDe csvDataSerDe;

    private CsvDataSerDe ensureCsvDataSerDe() {
        if (csvDataSerDe == null) {
            csvDataSerDe = new CsvDataSerDe();
            csvDataSerDe.setMetaData(this);
        }
        return csvDataSerDe;
    }

    @Override
    public Serializer getSerializer() {
        return ensureCsvDataSerDe();
    }

    @Override
    public Deserializer getDeserializer() {
        return ensureCsvDataSerDe();
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
                    ViewMetaData.class,
                    ViewMetaDataColumn.class,
            });
        }
        return xmlSerDe;
    }

    public static ViewMetaData fromXml(String xml) {
        return (ViewMetaData) getXmlSerDe().fromXML(xml);
    }

    public static ViewMetaData fromXmlFile(File xmlFile) {
        return (ViewMetaData) getXmlSerDe().fromXML(xmlFile);
    }
}
