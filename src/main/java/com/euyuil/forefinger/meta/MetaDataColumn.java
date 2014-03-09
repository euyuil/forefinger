package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.condition.EqualCondition;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamConverter;
import com.thoughtworks.xstream.converters.SingleValueConverter;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public abstract class MetaDataColumn {

    @XStreamAsAttribute
    private String name;

    @XStreamAsAttribute
    @XStreamConverter(TypeConverter.class)
    private Class type;

    public MetaDataColumn() {
    }

    public MetaDataColumn(String name, Class type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Class getType() {
        return type;
    }

    public abstract MetaData getMetaData();

    public EqualCondition isEqualTo(Object value) {
        return new EqualCondition(this, value);
    }

    public EqualCondition isEqualTo(MetaDataColumn column) {
        return new EqualCondition(this, column);
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
