package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamConverter;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
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
    private String[] columns;

    public TableIndex() {
    }

    public TableIndex(Type type, String column) {
        this.type = type;
        this.columns = new String[] { column };
    }

    public TableIndex(Type type, String[] columns) {
        this.type = type;
        this.columns = columns;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public static enum Type {
        PRIMARY, UNIQUE, INDEX, FULLTEXT
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
