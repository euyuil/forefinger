package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamConverter;
import com.thoughtworks.xstream.converters.SingleValueConverter;

/**
 * TODO Nullable and default.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
@XStreamAlias("column")
public class TableMetaDataColumn extends MetaDataColumn {

    /**
     * Constructs a TableMetaDataColumn object specifying TableMetaData object.
     * @param tableMetaData the TableMetaData object.
     */
    public TableMetaDataColumn(TableMetaData tableMetaData) {
        super(tableMetaData);
    }

    public TableMetaDataColumn(TableMetaData tableMetaData, String columnName, Class type) {
        super(tableMetaData);
        this.name = columnName; // Do not use setters because it will save schema multiple times.
        this.type = type; // Do not use setters because it will save schema multiple times.
        // TODO Save schema.
    }

    /**
     * The type of the table column.
     */
    @XStreamAsAttribute
    @XStreamConverter(TypeConverter.class)
    private Class type;

    @Override
    public Class getResultType() {
        return type;
    }

    public void setResultType(Class type) {
        this.type = type;
        // TODO Save meta data or something.
    }

    /**
     * Converts property 'type' to String and vice versa.
     * Used to do serializing and de-serializing.
     */
    public static class TypeConverter implements SingleValueConverter {

        @Override
        public String toString(Object o) {
            Class clazz = (Class) o;
            return clazz.getSimpleName();
        }

        @Override
        public Object fromString(String s) {
            String typeName = s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
            String typeFullName = String.format("java.lang.%s", typeName);
            try {
                return Class.forName(typeFullName);
            } catch (ClassNotFoundException e) {
                // TODO Error reporting.
                return null;
            }
        }

        @Override
        public boolean canConvert(Class clazz) {
            return (clazz.equals(Class.class));
        }
    }
}
