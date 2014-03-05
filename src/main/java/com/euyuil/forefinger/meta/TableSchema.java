package com.euyuil.forefinger.meta;

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
public class TableSchema implements DataSchema {

    @Override
    public DataColumn[] getDataColumns() {
        return new DataColumn[0];
    }

    public TableColumn[] getTableColumns() {
        return (TableColumn[]) getDataColumns();
    }
}
