package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.meta.condition.Condition;
import com.euyuil.forefinger.serde.CsvDataSerDe;
import com.euyuil.forefinger.serde.Deserializer;
import com.euyuil.forefinger.serde.Serializer;
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
@XStreamAlias("view")
public class ViewMetaData extends MetaData {

    /**
     * Constructs a ViewMetaData object specifying MetaDataSet object.
     * @param metaDataSet the MetaDataSet object.
     */
    public ViewMetaData(MetaDataSet metaDataSet) {
        super(metaDataSet);
    }

    /**
     * What's the key used for? The key is for data transportation between Map and Reduce job.
     */
    @XStreamAlias("usage")
    private KeyUsage keyUsage;

    public KeyUsage getKeyUsage() {
        return keyUsage;
    }

    public void setKeyUsage(KeyUsage keyUsage) {
        this.keyUsage = keyUsage;
        // TODO Save.
    }

    /**
     * Condition filters.
     */
    @XStreamAlias("where")
    private Condition condition;

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
        // TODO Save.
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

    /**
     * Joints. Used in data joining.
     */
    @XStreamImplicit(itemFieldName = "joint")
    private ArrayList<ViewMetaDataColumn> joints;

    public List<ViewMetaDataColumn> getJoints() {
        return Collections.unmodifiableList(joints);
    }

    public void setJoints(ArrayList<ViewMetaDataColumn> joints) {
        this.joints = joints;
        // TODO Save.
    }

    public boolean isTemporary() {
        return false; // TODO
    }

    /**
     * OrderBy options item.
     */
    public static class OrderByItem {

        private ViewMetaDataColumn column;

        private OrderType type;

        public OrderByItem(ViewMetaDataColumn column, OrderType type) {
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
    public static enum KeyUsage {
        NONE,
        ORDER,
        AGGREGATE,
        INNER_JOIN,
        LEFT_OUTER_JOIN,
        RIGHT_OUTER_JOIN,
        FULL_OUTER_JOIN
    }

    private CsvDataSerDe csvDataSerDe = new CsvDataSerDe();

    @Override
    public Serializer getSerializer() {
        return csvDataSerDe;
    }

    @Override
    public Deserializer getDeserializer() {
        return csvDataSerDe;
    }

    /**
     * @return the XML SerDe.
     */
    @Override
    protected XStream getStaticXmlSerDe() {
        return getXmlSerDe();
    }

    static XStream xmlSerDe;

    static XStream getXmlSerDe() {
        if (xmlSerDe == null) {
            xmlSerDe = new XStream();
            xmlSerDe.processAnnotations(new Class[]{
                    MetaData.class,
                    MetaDataColumn.class,
                    ViewMetaData.class,
                    ViewMetaDataColumn.class,
            });
        }
        return xmlSerDe;
    }

    public static ViewMetaData fromXml(String xml) {
        return (ViewMetaData) getXmlSerDe().fromXML(xml);
    }

    public static ViewMetaData fromXmlFile(File xmlFile) {
        return (ViewMetaData) getXmlSerDe().fromXML(xmlFile);
    }
}
