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
@XStreamAlias("orderView")
public class OrderViewMetaData extends SimpleViewMetaData {

    /**
     * Constructs a OrderViewMetaData object specifying MetaDataSet object.
     * @param metaDataSet the MetaDataSet object.
     */
    public OrderViewMetaData(MetaDataSet metaDataSet) {
        super(metaDataSet);
    }

    /**
     * OrderBy items.
     */
    @XStreamImplicit(itemFieldName = "orderBy")
    private ArrayList<OrderByItem> orderByItems; // TODO

    public List<OrderByItem> getOrderByItems() {
        return Collections.unmodifiableList(orderByItems);
    }

    public void setOrderByItems(ArrayList<OrderByItem> orderByItems) {
        this.orderByItems = orderByItems;
        // TODO Save.
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
