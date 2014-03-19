package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.function.Function;
import com.thoughtworks.xstream.annotations.XStreamAlias;

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

    /**
     * The name of the referenced data, could be name of a table or view.
     */
    @XStreamAlias("data")
    private String referenceMetaDataName;

    public MetaData getReferenceMetaData() {
        return getMetaDataSet().getMetaData(referenceMetaDataName);
    }

    public void setReferenceMetaData(MetaData metaData) {
        referenceMetaDataName = metaData.getName();
        // TODO Some invalidation job.
    }

    /**
     * The name of the column of the referenced data.
     */
    @XStreamAlias("column")
    private String referenceMetaDataColumnName;

    public MetaDataColumn getReferenceMetaDataColumn() {
        return getReferenceMetaData().getMetaDataColumn(referenceMetaDataColumnName);
    }

    public void setReferenceMetaDataColumn(MetaDataColumn metaDataColumn) {
        // Do not use setReferenceMetaData because it will invalidate this and we don't need to do twice.
        referenceMetaDataName = metaDataColumn.getMetaData().getName();
        referenceMetaDataColumnName = metaDataColumn.getName();
        // TODO Some invalidation job.
    }

    @Override
    public Class getResultType() {
        if (function == null) {
            return getReferenceMetaDataColumn().getResultType();
        } else {
            return function.getResultType();
        }
    }
}
