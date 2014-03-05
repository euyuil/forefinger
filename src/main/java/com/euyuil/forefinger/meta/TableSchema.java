package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

/**
 * The schema of the table.
 * It's size should be really small.
 * And its configuration should be managed by clients.
 * So it's synchronized between nodes.
 * And it's stored in local file system of each node.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
@XStreamAlias("schema")
public class TableSchema implements DataSchema {

    @XStreamImplicit(itemFieldName = "column")
    private TableColumn[] columns;

    @XStreamImplicit(itemFieldName = "index")
    private TableIndex[] indices;

    @Override
    public DataColumn[] getDataColumns() {
        return columns;
    }

    public TableColumn[] getTableColumns() {
        return columns;
    }

    public void setTableColumns(TableColumn[] tableColumns) {
        this.columns = tableColumns;
    }

    public TableIndex[] getTableIndices() {
        return indices;
    }

    public void setTableIndices(TableIndex[] tableIndices) {
        this.indices = tableIndices;
    }
}
