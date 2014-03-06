package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

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
public class MetaTable extends MetaData {

    static final XStream xmlSerDe = new XStream();

    static {
        xmlSerDe.processAnnotations(new Class[]{
                MetaData.class,
                MetaDataColumn.class,
                MetaTable.class,
                MetaTableColumn.class,
                MetaTableIndex.class,
        });
    }

    public static MetaTable fromXml(String xml) {
        return (MetaTable) xmlSerDe.fromXML(xml);
    }

    public static String toXml(MetaTable table) {
        return xmlSerDe.toXML(table);
    }

    @XStreamImplicit(itemFieldName = "column")
    private MetaTableColumn[] columns;

    @XStreamImplicit(itemFieldName = "index")
    private MetaTableIndex[] indices;

    /**
     * The paths of the table data sets.
     */
    @XStreamImplicit(itemFieldName = "path")
    private String[] paths;

    @Override
    public MetaDataColumn[] getDataColumns() {
        return columns;
    }

    public MetaTableColumn[] getTableColumns() {
        return columns;
    }

    public void setTableColumns(MetaTableColumn[] columns) {
        this.columns = columns;
    }

    public MetaTableIndex[] getTableIndices() {
        return indices;
    }

    public void setTableIndices(MetaTableIndex[] tableIndices) {
        this.indices = tableIndices;
    }

    public String[] getPaths() {
        return paths;
    }

    public void setPaths(String[] paths) {
        this.paths = paths;
    }
}
