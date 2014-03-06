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
public class TableIndex {

    @XStreamAsAttribute
    @XStreamConverter(TypeConverter.class)
    private Type type;

    @XStreamImplicit(itemFieldName = "column")
    private Column[] columns;

    public TableIndex() {
    }

    public TableIndex(Type type, Column column) {
        this.type = type;
        this.columns = new Column[] { column };
    }

    public TableIndex(Type type, String column) {
        this.type = type;
        this.columns = new Column[] { new Column(column) };
    }

    public TableIndex(Type type, Column[] columns) {
        this.type = type;
        this.columns = columns;
    }

    public TableIndex(Type type, String[] columns) {
        this.type = type;
        this.columns = new Column[columns.length];
        for (int i = 0; i < columns.length; ++i)
            this.columns[i] = new Column(columns[i]);
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Column[] getColumns() {
        return columns;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    public static enum Type {
        PRIMARY, UNIQUE, INDEX, FULLTEXT
    }

    @XStreamAlias("column")
    public static class Column {

        @XStreamOmitField
        public final int DEFAULT_SIZE = 256;

        @XStreamAsAttribute
        private String name;

        @XStreamAsAttribute
        private int size;

        public Column() {
            this.size = DEFAULT_SIZE;
        }

        public Column(String name) {
            this.name = name;
            this.size = DEFAULT_SIZE;
        }

        public Column(String name, int size) {
            this.name = name;
            this.size = size;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
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
