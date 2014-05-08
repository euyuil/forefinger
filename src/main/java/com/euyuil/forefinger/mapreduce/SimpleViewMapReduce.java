package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.meta.MetaData;
import com.euyuil.forefinger.meta.MetaDataColumn;
import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.meta.condition.Condition;
import com.euyuil.forefinger.meta.view.SimpleViewMetaData;
import com.euyuil.forefinger.meta.view.ViewMetaDataColumn;
import com.euyuil.forefinger.serde.ArrayDataRow;
import com.euyuil.forefinger.serde.DataRow;
import com.euyuil.forefinger.serde.Deserializer;
import com.euyuil.forefinger.serde.Serializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Just project something with some filter.
 *
 * Input view could be a very SimpleViewMetaData.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.27
 */
public abstract class SimpleViewMapReduce extends ForefingerMapReduce {

    public static final String PARAM_SIMPLE_VIEW_NAME = "forefinger.simple.view.name";

    public static boolean startAndWaitForSimpleViewJob(SimpleViewMetaData simpleViewMetaData) {
        return false;
    }

    public static SimpleViewMetaData getSimpleViewMetaData(Configuration configuration) {
        String name = configuration.get(PARAM_SIMPLE_VIEW_NAME);
        if (name == null || name.length() == 0)
            return null;
        return MetaDataSet.getDefault().getMetaData(name, SimpleViewMetaData.class);
    }

    public static class SimpleViewMapper
            extends ForefingerMapper<LongWritable, Text, LongWritable, Text> {

        private SimpleViewMetaData simpleViewMetaData;

        private Condition condition;

        private Serializer serializer;

        private Deserializer deserializer;

        private List<MetaDataColumn> metaDataColumns;

        private MetaDataSet metaDataSet;

        private Text text = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            simpleViewMetaData = getSimpleViewMetaData(context.getConfiguration());
            serializer = simpleViewMetaData.getSerializer();
            deserializer = simpleViewMetaData.getDeserializer();
            condition = simpleViewMetaData.getCondition();
            metaDataColumns = Collections.unmodifiableList(simpleViewMetaData.getMetaDataColumns());
            metaDataSet = simpleViewMetaData.getMetaDataSet();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            DataRow dataRow = deserializer.deserialize(value.toString());

            // Filter conditions.
            if (condition != null && !condition.fulfilled(dataRow))
                return;

            // Projections.
            ArrayDataRow writeDataRow = new ArrayDataRow(metaDataColumns.size());
            for (int columnIndex = 0; columnIndex < metaDataColumns.size(); columnIndex++) {
                ViewMetaDataColumn column = (ViewMetaDataColumn) metaDataColumns.get(columnIndex);
                // TODO Cache this in setup().
                String sourceDataName = column.getSourceDataName();
                if (sourceDataName == null || sourceDataName.length() == 0)
                    sourceDataName = simpleViewMetaData.getSource().getName();
                MetaData sourceData = metaDataSet.getMetaData(sourceDataName);
                int sourceColumnIndex = sourceData.getMetaDataColumnIndex(column.getSourceColumnName());
                writeDataRow.set(columnIndex, dataRow.get(sourceColumnIndex));
            }

            // Write results.
            String serialized = serializer.serialize(writeDataRow);
            text.set(serialized);
            context.write(key, text);
        }
    }

    public static class SimpleViewReducer
            extends ForefingerReducer<LongWritable, Text, NullWritable, Text> {

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text text : values) // TODO Use the serializer and deserializer.
                context.write(NullWritable.get(), text);
        }
    }
}
