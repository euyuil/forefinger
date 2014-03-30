package com.euyuil.forefinger.meta.view;

import com.euyuil.forefinger.meta.MetaData;
import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.serde.Deserializer;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.29
 */
@XStreamAlias("orderView")
public class OrderViewMetaData extends ViewMetaData {

    /**
     * OrderBy items.
     */
    @XStreamImplicit(itemFieldName = "orderBy")
    private ArrayList<OrderByItem> orderByItems; // TODO
    /**
     * Joints. Used in data joining.
     */
    @XStreamImplicit(itemFieldName = "joint")
    private ArrayList<JoinViewMetaData.JoinItem> joinItems;
    /**
     * Join type for each source data, inner or outer.
     */
    private ArrayList<JoinViewMetaData.JoinType> joinTypes;
    /**
     * GroupBy columns. Should NOT set them again in the standard column property.
     */
    @XStreamImplicit(itemFieldName = "groupBy")
    private ArrayList<ViewMetaDataColumn> groupByColumns; // TODO
    /**
     * The name of the referenced data, could be name of a table or view.
     * A view could have multiple sources, but they should have the same schema.
     * TODO Check if the schemas are the same.
     */
    @XStreamImplicit(itemFieldName = "from")
    private ArrayList<String> sources;
    @XStreamOmitField
    private ArrayList<MetaData> sourcesCache;

    /**
     * Constructs a OrderViewMetaData object specifying MetaDataSet object.
     * @param metaDataSet the MetaDataSet object.
     */
    public OrderViewMetaData(MetaDataSet metaDataSet) {
        super(metaDataSet);
    }

    public List<OrderByItem> getOrderByItems() {
        return Collections.unmodifiableList(orderByItems);
    }

    public void setOrderByItems(ArrayList<OrderByItem> orderByItems) {
        this.orderByItems = orderByItems;
        // TODO Save.
    }

    public List<JoinViewMetaData.JoinItem> getJoinItems() {
        return Collections.unmodifiableList(joinItems);
    }

    public void setJoinItems(ArrayList<JoinViewMetaData.JoinItem> joinItems) {
        this.joinItems = joinItems;
        // TODO Save.
    }

    public List<JoinViewMetaData.JoinType> getJoinTypes() {
        return Collections.unmodifiableList(joinTypes);
    }

    public void setJoinTypes(ArrayList<JoinViewMetaData.JoinType> joinTypes) {
        this.joinTypes = joinTypes;
    }

    public List<ViewMetaDataColumn> getGroupByColumns() {
        return Collections.unmodifiableList(groupByColumns);
    }

    public void setGroupByColumns(ArrayList<ViewMetaDataColumn> groupByColumns) {
        this.groupByColumns = groupByColumns;
        // TODO Save.
    }

    public List<MetaData> getSources() {
        if (sources != null && sourcesCache == null)
            invalidateSourcesCache();
        return sourcesCache;
    }

    public MetaData getSource() {
        return getSources().get(0);
    }

    public void setSources(List<MetaData> sources) {
        // This should not be deep copy, but shallow copy. It's desired behavior.
        sourcesCache = new ArrayList<MetaData>(sources);
        this.sources = new ArrayList<String>(sources.size());
        for (MetaData source : sourcesCache)
            this.sources.add(source.getName());
        // TODO Some invalidation job.
    }

    private void invalidateSourcesCache() {
        ArrayList<MetaData> newSourcesCache = new ArrayList<MetaData>(sources.size());
        for (String source : sources) {
            newSourcesCache.add(getMetaDataSet().getMetaData(source));
        }
        sourcesCache = newSourcesCache;
    }

    @Override
    public Deserializer getDeserializer() {
        MetaData source = getSource();
        if (source != null && source.getDeserializer() != null)
            return source.getDeserializer();
        return super.getDeserializer();
    }

    /**
     * OrderBy options item.
     */
    public static class OrderByItem {

        private String columnName;

        private OrderType type;

        public OrderByItem(String columnName, OrderType type) {
            this.columnName = columnName;
            this.type = type;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public OrderType getOrderType() {
            return type;
        }

        public void setType(OrderType type) {
            this.type = type;
        }

        public static enum OrderType {
            ASCENDING,
            DESCENDING
        }
    }
}
