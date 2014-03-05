package com.euyuil.forefinger.meta;

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
public class Table implements Data {

    /**
     * The name of the table.
     */
    private String name;

    /**
     * The paths of the table data sets.
     */
    @XStreamImplicit(itemFieldName = "path")
    private String[] paths;

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

    public String[] getPaths() {
        return paths;
    }

    public void setPaths(String[] paths) {
        this.paths = paths;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public void setSchema(TableSchema schema) {
        this.schema = schema;
    }
}
