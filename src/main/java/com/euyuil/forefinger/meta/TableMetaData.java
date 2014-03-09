package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import java.io.File;

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

    static final XStream xmlSerDe = new XStream();

    static {
        xmlSerDe.processAnnotations(new Class[]{
                MetaData.class,
                MetaDataColumn.class,
                TableMetaData.class,
                TableMetaDataColumn.class,
                TableMetaDataIndex.class,
        });
    }

    public static TableMetaData fromXml(String xml) {
        return (TableMetaData) xmlSerDe.fromXML(xml);
    }

    public static TableMetaData fromXmlFile(File xmlFile) {
        return (TableMetaData) xmlSerDe.fromXML(xmlFile);
    }

    @XStreamImplicit(itemFieldName = "column")
    private TableMetaDataColumn[] columns;

    @XStreamImplicit(itemFieldName = "index")
    private TableMetaDataIndex[] indices;

    /**
     * The paths of the table data sets.
     */
    @XStreamImplicit(itemFieldName = "path")
    private String[] paths;

    /**
     * Returns all the columns to the select query (projection).
     * @return a MetaDataColumn object stands for all the columns.
     */
    public MetaDataColumn allColumns() {
        return null; // TODO
    }

    @Override
    public MetaDataColumn[] getDataColumns() {
        return columns;
    }

    public TableMetaDataColumn[] getTableColumns() {
        return columns;
    }

    public void setTableColumns(TableMetaDataColumn[] columns) {
        this.columns = columns;
    }

    @Override
    public MetaDataColumn getDataColumn(String columnName) {
        return getTableColumn(columnName);
    }

    public TableMetaDataColumn getTableColumn(String columnName) {
        return null; // TODO
    }

    public TableMetaDataIndex[] getTableIndices() {
        return indices;
    }

    public void setTableIndices(TableMetaDataIndex[] tableIndices) {
        this.indices = tableIndices;
    }

    public String[] getPaths() {
        return paths;
    }

    public void setPaths(String[] paths) {
        this.paths = paths;
    }

    @Override
    protected XStream getXmlSerDe() {
        return xmlSerDe;
    }
}
