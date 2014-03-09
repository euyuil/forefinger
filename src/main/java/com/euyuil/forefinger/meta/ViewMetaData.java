package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import java.io.File;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
@XStreamAlias("view")
public class ViewMetaData extends MetaData {

    static final XStream xmlSerDe = new XStream();

    static {
        xmlSerDe.processAnnotations(new Class[]{
                MetaData.class,
                MetaDataColumn.class,
                ViewMetaData.class,
                ViewMetaDataColumn.class,
        });
    }

    public static ViewMetaData fromXml(String xml) {
        return (ViewMetaData) xmlSerDe.fromXML(xml);
    }

    public static ViewMetaData fromXmlFile(File xmlFile) {
        return (ViewMetaData) xmlSerDe.fromXML(xmlFile);
    }

    @XStreamImplicit(itemFieldName = "column")
    private ViewMetaDataColumn[] columns;

    public ViewMetaDataColumn[] getViewColumns() {
        return columns;
    }

    public void setViewColumns(ViewMetaDataColumn[] columns) {
        this.columns = columns;
    }

    @Override
    public MetaDataColumn[] getDataColumns() {
        return columns;
    }

    @Override
    public MetaDataColumn getDataColumn(String columnName) {
        return null;
    }

    @Override
    protected XStream getXmlSerDe() {
        return xmlSerDe;
    }
}
