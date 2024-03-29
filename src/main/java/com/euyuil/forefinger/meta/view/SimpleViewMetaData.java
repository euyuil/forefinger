package com.euyuil.forefinger.meta.view;

import com.euyuil.forefinger.meta.MetaData;
import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.serde.Deserializer;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.29
 */
@XStreamAlias("simpleView")
public class SimpleViewMetaData extends ViewMetaData {

    /**
     * Constructs a SimpleViewMetaData object specifying MetaDataSet object.
     * @param metaDataSet the MetaDataSet object.
     */
    public SimpleViewMetaData(MetaDataSet metaDataSet) {
        super(metaDataSet);
    }

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
     * @return the source list.
     */
    public List<MetaData> getSources() {
        if (sources != null && sourcesCache == null)
            invalidateSourcesCache();
        return sourcesCache;
    }

    /**
     * @return the default (i.e. the 0th) source.
     */
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
}
