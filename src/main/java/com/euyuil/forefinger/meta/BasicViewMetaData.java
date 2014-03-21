package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.meta.condition.Condition;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
@XStreamAlias("basicView")
public class BasicViewMetaData extends MetaData {

    /**
     * Constructs a BasicViewMetaData object specifying MetaDataSet object.
     * @param metaDataSet the MetaDataSet object.
     */
    public BasicViewMetaData(MetaDataSet metaDataSet) {
        super(metaDataSet);
    }

    @XStreamAlias("type")
    private KeyType keyType;

    public KeyType getKeyType() {
        return keyType;
    }

    public void setKeyType(KeyType keyType) {
        this.keyType = keyType;
        // TODO Save.
    }

    @XStreamAlias("where")
    private Condition condition;

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
        // TODO Save.
    }

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
     * GroupBy columns. Should NOT set them again in the standard column property.
     */
    @XStreamImplicit(itemFieldName = "groupBy")
    private ArrayList<BasicViewMetaDataColumn> groupByColumns; // TODO

    public List<BasicViewMetaDataColumn> getGroupByColumns() {
        return Collections.unmodifiableList(groupByColumns);
    }

    public void setGroupByColumns(ArrayList<BasicViewMetaDataColumn> groupByColumns) {
        this.groupByColumns = groupByColumns;
        // TODO Save.
    }

    public boolean isMaterial() {
        return false; // TODO
    }

    public boolean isTemporary() {
        return false; // TODO
    }

    /**
     * OrderBy options item.
     */
    public static class OrderByItem {

        private BasicViewMetaDataColumn column;

        private OrderType type;

        public OrderByItem(BasicViewMetaDataColumn column, OrderType type) {
            this.column = column;
            this.type = type;
        }

        public static enum OrderType {
            ASCENDING,
            DESCENDING
        }
    }

    /**
     * The meaning of the key of the intermediate result between a Map and Reduce job.
     */
    public static enum KeyType {
        ORDER,
        AGGREGATE,
        INNER_JOIN,
        LEFT_OUTER_JOIN,
        RIGHT_OUTER_JOIN,
        FULL_OUTER_JOIN
    }

    /**
     * @return the XML SerDe.
     */
    @Override
    protected XStream getXmlSerDe() {
        return xmlSerDe;
    }

    static final XStream xmlSerDe = new XStream();

    static {
        xmlSerDe.processAnnotations(new Class[]{
                MetaData.class,
                MetaDataColumn.class,
                BasicViewMetaData.class,
                BasicViewMetaDataColumn.class,
        });
    }

    public static BasicViewMetaData fromXml(String xml) {
        return (BasicViewMetaData) xmlSerDe.fromXML(xml);
    }

    public static BasicViewMetaData fromXmlFile(File xmlFile) {
        return (BasicViewMetaData) xmlSerDe.fromXML(xmlFile);
    }
}
