package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.meta.*;
import com.euyuil.forefinger.meta.condition.Condition;
import com.euyuil.forefinger.serde.ArrayDataRow;
import com.euyuil.forefinger.serde.DataRow;
import com.euyuil.forefinger.serde.Deserializer;
import com.euyuil.forefinger.serde.Serializer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.19
 */
public class ViewMapReduce {

    public static String PARAM_VIEW_NAME = "viewName";

    public static class ViewMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private Text text = new Text();
        private CompositeWritable compositeWritable;

        private MetaData viewMetaDataSource;
        private List<MetaDataColumn> viewMetaDataSourceColumns;

        private ViewMetaData viewMetaData;
        private ViewMetaData.KeyUsage viewMetaDataKeyUsage;
        private Serializer viewMetaDataSerializer;
        private Deserializer viewMetaDataDeserializer;
        private Condition viewMetaDataCondition;
        private List<MetaDataColumn> viewMetaDataColumns;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            String viewName = context.getConfiguration().get(PARAM_VIEW_NAME);
            viewMetaData = MetaDataSet.getDefault().getMetaData(viewName, ViewMetaData.class);

            viewMetaDataSource = viewMetaData.getSource();
            viewMetaDataSourceColumns = viewMetaDataSource.getMetaDataColumns();

            viewMetaDataKeyUsage = viewMetaData.getKeyUsage();
            viewMetaDataSerializer = viewMetaData.getSerializer();
            viewMetaDataDeserializer = viewMetaData.getDeserializer();
            viewMetaDataCondition = viewMetaData.getCondition();
            viewMetaDataColumns = viewMetaData.getMetaDataColumns();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (viewMetaDataKeyUsage == ViewMetaData.KeyUsage.NONE) {
                DataRow dataRow = viewMetaDataDeserializer.deserialize(value.toString());

                // Filter conditions.
                if (viewMetaDataCondition != null && !viewMetaDataCondition.fulfilled(dataRow))
                    return;

                // Projections.
                ArrayDataRow writeDataRow = new ArrayDataRow(viewMetaData.getMetaDataColumns().size());
                for (int columnIndex = 0; columnIndex < viewMetaDataColumns.size(); columnIndex++) {
                    ViewMetaDataColumn column = (ViewMetaDataColumn) viewMetaDataColumns.get(columnIndex);
                    int sourceColumnIndex = viewMetaDataSource.getMetaDataColumnIndex(column.getSourceColumnName());
                    writeDataRow.set(columnIndex, dataRow.get(sourceColumnIndex));
                }

                // Write results.
                String serialized = viewMetaDataSerializer.serialize(writeDataRow);
                text.set(serialized);
                context.write(key, text);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
}
