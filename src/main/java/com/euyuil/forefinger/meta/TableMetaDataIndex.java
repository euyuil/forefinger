package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.annotations.*;
import com.thoughtworks.xstream.converters.SingleValueConverter;

/**
 * The index of the table.
 * Its meta data is small,
 * but the index itself could be very large.
 * The meta data is stored with schema.
 * And the index content is stored in HDFS.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
@XStreamAlias("index")
public class TableMetaDataIndex {

    @XStreamAsAttribute
    @XStreamConverter(TypeConverter.class)
    private Type type;

    @XStreamImplicit
    private IndexItem[] indexItems;

    public TableMetaDataIndex() {
    }

    public TableMetaDataIndex(Type type, IndexItem indexItem) {
        this.type = type;
        this.indexItems = new IndexItem[] {indexItem};
    }

    public TableMetaDataIndex(Type type, String column) {
        this.type = type;
        this.indexItems = new IndexItem[] { new IndexItem(column) };
    }

    public TableMetaDataIndex(Type type, IndexItem[] indexItems) {
        this.type = type;
        this.indexItems = indexItems;
    }

    public TableMetaDataIndex(Type type, String[] columns) {
        this.type = type;
        this.indexItems = new IndexItem[columns.length];
        for (int i = 0; i < columns.length; ++i)
            this.indexItems[i] = new IndexItem(columns[i]);
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public IndexItem[] getIndexItems() {
        return indexItems;
    }

    public void setIndexItems(IndexItem[] indexItems) {
        this.indexItems = indexItems;
    }

    public static enum Type {
        PRIMARY, UNIQUE, INDEX, FULLTEXT
    }

    @XStreamAlias("item")
    public static class IndexItem {

        @XStreamOmitField
        public final int DEFAULT_SIZE = 256;

        @XStreamAsAttribute
        private String column;

        @XStreamAsAttribute
        private int size;

        public IndexItem() {
            this.size = DEFAULT_SIZE;
        }

        public IndexItem(String column) {
            this.column = column;
            this.size = DEFAULT_SIZE;
        }

        public IndexItem(String column, int size) {
            this.column = column;
            this.size = size;
        }

        public String getColumn() {
            return column;
        }

        public void setColumn(String column) {
            this.column = column;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }
    }

    public static class TypeConverter implements SingleValueConverter {

        @Override
        public String toString(Object o) {
            Type type = (Type) o;
            String prettyName = type.name().substring(0, 1).toUpperCase() +
                    type.name().substring(1).toLowerCase();
            if (prettyName.equalsIgnoreCase("Fulltext"))
                prettyName = "FullText";
            return prettyName;
        }

        @Override
        public Object fromString(String s) {
            return Type.valueOf(s.toUpperCase());
        }

        @Override
        public boolean canConvert(Class clazz) {
            return clazz.equals(Type.class);
        }
    }
}
