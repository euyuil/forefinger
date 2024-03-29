package com.euyuil.forefinger.meta.view;

import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.serde.Deserializer;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.29
 */
@XStreamAlias("joinView")
public class JoinViewMetaData extends ViewMetaData {

    /**
     * Joints. Used in data joining.
     */
    @XStreamImplicit(itemFieldName = "join")
    private ArrayList<JoinItem> joinItems;

    /**
     * The name of the referenced data, could be name of a table or view.
     * A view could have multiple sources, but they should have the same schema.
     * TODO Check if the schemas are the same.
     */
    @XStreamImplicit(itemFieldName = "joinFrom")
    private ArrayList<JoinSource> sources;

    /**
     * Constructs a JoinViewMetaData object specifying MetaDataSet object.
     * @param metaDataSet the MetaDataSet object.
     */
    public JoinViewMetaData(MetaDataSet metaDataSet) {
        super(metaDataSet);
    }

    public List<JoinItem> getJoinItems() {
        return Collections.unmodifiableList(joinItems);
    }

    public void setJoinItems(ArrayList<JoinItem> joinItems) {
        this.joinItems = joinItems;
        // TODO Save.
    }

    public List<JoinSource> getSources() {
        return Collections.unmodifiableList(sources);
    }

    public void setSources(ArrayList<JoinSource> sources) {
        this.sources = sources;
    }

    @Override
    public Deserializer getDeserializer() {
        throw new UnsupportedOperationException("Use getDeserializer(identity)");
    }

    public Deserializer getDeserializer(int identity) {
        return getMetaDataSet().getMetaData(sources.get(identity).getDataName()).getDeserializer();
    }

    public static enum JoinType {
        INNER, OUTER
    }

    public static class JoinItem {

        @XStreamImplicit(itemFieldName = "joinItemColumn")
        private ArrayList<Column> columns;

        public static class Column {

            private String dataName;

            private String columnName;

            public Column() {
            }

            public Column(String dataName, String columnName) {
                this.dataName = dataName;
                this.columnName = columnName;
            }

            public String getDataName() {
                return dataName;
            }

            public void setDataName(String dataName) {
                this.dataName = dataName;
            }

            public String getColumnName() {
                return columnName;
            }

            public void setColumnName(String columnName) {
                this.columnName = columnName;
            }
        }

        public JoinItem() {
        }

        public JoinItem(ArrayList<Column> columns) {
            this.columns = columns;
        }

        public List<Column> getColumns() {
            return Collections.unmodifiableList(columns);
        }

        public void setColumns(ArrayList<Column> columns) {
            this.columns = columns;
        }
    }

    public static class JoinSource {

        private String dataName;

        private JoinType joinType;

        public JoinSource() {
        }

        public JoinSource(String dataName, JoinType joinType) {
            this.dataName = dataName;
            this.joinType = joinType;
        }

        public String getDataName() {
            return dataName;
        }

        public void setDataName(String dataName) {
            this.dataName = dataName;
        }

        public JoinType getJoinType() {
            return joinType;
        }

        public void setJoinType(JoinType joinType) {
            this.joinType = joinType;
        }
    }
}
