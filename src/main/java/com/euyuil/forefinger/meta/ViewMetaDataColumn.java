package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.meta.function.Function;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;

/**
 * ViewMetaData projection with convert functions.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
@XStreamAlias("column")
public class ViewMetaDataColumn extends MetaDataColumn {

    public ViewMetaDataColumn(ViewMetaData viewMetaData) {
        super(viewMetaData);
    }

    public ViewMetaDataColumn(ViewMetaData viewMetaData, String name, String sourceColumnName) {
        super(viewMetaData, name);
        setSourceColumnName(sourceColumnName);
    }

    public ViewMetaDataColumn(ViewMetaData viewMetaData, String name, String sourceColumnName, Function function) {
        super(viewMetaData, name);
        setSourceColumnName(sourceColumnName);
        setFunction(function);
    }

    /**
     * The function that converts the column data to other desired value.
     */
    @XStreamAlias("function")
    private Function function;

    public Function getFunction() {
        return function;
        // TODO XML converter.
    }

    public void setFunction(Function function) {
        this.function = function;
        // TODO Save schema or something.
    }

    @XStreamAsAttribute
    @XStreamAlias("source")
    private String sourceColumnName;

    public String getSourceColumnName() {
        return sourceColumnName;
    }

    public void setSourceColumnName(String sourceColumnName) {
        this.sourceColumnName = sourceColumnName;
    }

    /**
     * @return the ViewMetaData of this column.
     */
    public ViewMetaData getViewMetaData() {
        return (ViewMetaData) getMetaData();
    }

    @Override
    public Class getResultType() {
        if (function == null) {
            return getViewMetaData().getSource().getMetaDataColumn(getSourceColumnName()).getResultType();
        } else {
            return function.getResultType();
        }
    }
}
