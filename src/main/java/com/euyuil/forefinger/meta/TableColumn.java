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
public class TableColumn extends DataColumn {

    @XStreamAsAttribute
    private String name;

    @XStreamAsAttribute
    @XStreamConverter(TypeConverter.class)
    private Class type;

    public TableColumn() {
    }

    public TableColumn(String name, Class type) {
        this.name = name;
        this.type = type;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Class getType() {
        return type;
    }

    public void setType(Class type) {
        this.type = type;
    }

    public static class TypeConverter implements SingleValueConverter {

        @Override
        public String toString(Object o) {
            Class clazz = (Class) o;
            return clazz.getSimpleName();
        }

        @Override
        public Object fromString(String s) {
            String typeName = s.substring(0, 1).toUpperCase() +
                    s.substring(1).toLowerCase();
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
