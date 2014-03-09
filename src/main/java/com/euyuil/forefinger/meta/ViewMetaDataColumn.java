package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.converter.Converter;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
@XStreamAlias("column")
public class ViewMetaDataColumn extends MetaDataColumn {

    private Converter converter;

    private String name;

    public ViewMetaDataColumn() {
    }

    public ViewMetaDataColumn(String name, Class type) {
        super(name, type);
    }

    public ViewMetaData getViewMetaData() {
        return null;
    }

    public MetaDataColumn getSourceMetaDataColumn() {
        return null;
    }

    public Converter getConverter() {
        return converter;
    }

    public String getName() {
        return name;
    }

    @Override
    public MetaData getMetaData() {
        return null;
    }
}
