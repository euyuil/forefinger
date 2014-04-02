package com.euyuil.forefinger.meta.view;

import com.euyuil.forefinger.meta.MetaDataSet;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.29
 */
@XStreamAlias("aggregateView")
public class AggregateViewMetaData extends SimpleViewMetaData {

    /**
     * Constructs a AggregateViewMetaData object specifying MetaDataSet object.
     * @param metaDataSet the MetaDataSet object.
     */
    public AggregateViewMetaData(MetaDataSet metaDataSet) {
        super(metaDataSet);
    }

    /**
     * GroupBy columns. Should NOT set them again in the standard column property.
     */
    @XStreamImplicit(itemFieldName = "groupBy")
    private ArrayList<ViewMetaDataColumn> groupByColumns; // TODO

    public List<ViewMetaDataColumn> getGroupByColumns() {
        return Collections.unmodifiableList(groupByColumns);
    }

    public void setGroupByColumns(ArrayList<ViewMetaDataColumn> groupByColumns) {
        this.groupByColumns = groupByColumns;
        // TODO Save.
    }
}
