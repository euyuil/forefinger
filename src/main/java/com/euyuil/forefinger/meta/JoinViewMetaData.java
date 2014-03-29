package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.serde.Deserializer;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.29
 */
public class JoinViewMetaData extends ViewMetaData {

    /**
     * Joints. Used in data joining.
     */
    @XStreamImplicit(itemFieldName = "joint")
    private ArrayList<JointItem> jointItems;
    /**
     * Join type for each source data, inner or outer.
     */
    private ArrayList<JoinType> joinTypes;
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
     * Constructs a JoinViewMetaData object specifying MetaDataSet object.
     * @param metaDataSet the MetaDataSet object.
     */
    public JoinViewMetaData(MetaDataSet metaDataSet) {
        super(metaDataSet);
    }

    public List<JointItem> getJointItems() {
        return Collections.unmodifiableList(jointItems);
    }

    public void setJointItems(ArrayList<JointItem> jointItems) {
        this.jointItems = jointItems;
        // TODO Save.
    }

    public List<JoinType> getJoinTypes() {
        return Collections.unmodifiableList(joinTypes);
    }

    public void setJoinTypes(ArrayList<JoinType> joinTypes) {
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
        return ensureCsvDataSerDe();
    }

    public static enum JoinType {
        INNER, OUTER
    }

    public static class JointItem {

        private String dataName;

        private String columnName;
    }
}
