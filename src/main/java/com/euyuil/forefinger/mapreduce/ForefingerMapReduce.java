package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.meta.MetaData;
import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.meta.TableMetaData;
import com.euyuil.forefinger.meta.view.ViewMetaData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Liu Yue
 * @version 0.0.2014.05.01
 */
public class ForefingerMapReduce {

    public static final String PARAM_META_NAME = "forefinger.meta.name";

    public static final String PARAM_TABLE_NAME = "forefinger.table.name";

    public static final String PARAM_VIEW_NAME = "forefinger.view.name";

    public static MetaData getMetaData(Configuration configuration) {
        String metaName = configuration.get(PARAM_META_NAME);
        if (metaName == null || metaName.length() == 0)
            return null;
        return MetaDataSet.getDefault().getMetaData(metaName);
    }

    public static TableMetaData getTableMetaData(Configuration configuration) {
        String tableName = configuration.get(PARAM_TABLE_NAME);
        if (tableName == null || tableName.length() == 0)
            return null;
        return MetaDataSet.getDefault().getMetaData(tableName, TableMetaData.class);
    }

    public static ViewMetaData getViewMetaData(Configuration configuration) {
        String viewName = configuration.get(PARAM_VIEW_NAME);
        if (viewName == null || viewName.length() == 0)
            return null;
        return MetaDataSet.getDefault().getMetaData(viewName, ViewMetaData.class);
    }

    public static class ForefingerMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
            extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

        private MetaData metaData;
        private TableMetaData tableMetaData;
        private ViewMetaData viewMetaData;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            metaData = ForefingerMapReduce.getMetaData(context.getConfiguration());
            tableMetaData = ForefingerMapReduce.getTableMetaData(context.getConfiguration());
            viewMetaData = ForefingerMapReduce.getViewMetaData(context.getConfiguration());
        }

        protected MetaData getMetaData() {
            return metaData;
        }

        protected TableMetaData getTableMetaData() {
            return tableMetaData;
        }

        protected ViewMetaData getViewMetaData() {
            return viewMetaData;
        }
    }

    public static class ForefingerReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
            extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

        private MetaData metaData;
        private TableMetaData tableMetaData;
        private ViewMetaData viewMetaData;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            metaData = ForefingerMapReduce.getMetaData(context.getConfiguration());
            tableMetaData = ForefingerMapReduce.getTableMetaData(context.getConfiguration());
            viewMetaData = ForefingerMapReduce.getViewMetaData(context.getConfiguration());
        }

        protected MetaData getMetaData() {
            return metaData;
        }

        protected TableMetaData getTableMetaData() {
            return tableMetaData;
        }

        protected ViewMetaData getViewMetaData() {
            return viewMetaData;
        }
    }
}
