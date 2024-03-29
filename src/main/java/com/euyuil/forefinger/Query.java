package com.euyuil.forefinger;

import com.euyuil.forefinger.meta.*;
import com.euyuil.forefinger.meta.condition.Condition;
import com.euyuil.forefinger.meta.view.ViewMetaData;
import org.apache.hadoop.mapred.jobcontrol.Job;

import java.util.*;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public class Query {

    public static Query select(List<ViewMetaData> projection) {
        Query query = new Query();
        query.projection.addAll(projection);
        return query;
    }

    public static Query select(ViewMetaData... projection) {
        return select(Arrays.asList(projection));
    }

    public static Query select(MetaDataColumn... projection) {
        return new Query();
    }

    public static Query selectAll(MetaData metaData) {
        return new Query();
    }

    private MetaDataSet metaDataSet;

    /**
     * The projection of the query.
     */
    private ArrayList<ViewMetaData> projection =
            new ArrayList<ViewMetaData>();

    private Condition selection;

    public Query where(Condition selection) {
        this.selection = selection;
        return this;
    }

    public Job build() {

        Map<String, TableMetaData> involvedTables = new HashMap<String, TableMetaData>();
        Map<String, MetaData> involvedData = new HashMap<String, MetaData>();

        for (ViewMetaData viewMetaData : projection)
            involvedData.put(viewMetaData.getName(), viewMetaData);

        List<TableMetaData> tableMetaDataList =
                getUnderlyingTableMetaDataList(involvedData.values());

        for (TableMetaData tableMetaData : tableMetaDataList)
            involvedTables.put(tableMetaData.getName(), tableMetaData);

        return null;
    }

    private static ArrayList<TableMetaData> getUnderlyingTableMetaDataList(
            Iterable<MetaData> setOfMetaData) {
        return null;
    }
}
