package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.meta.ViewMetaData;
import com.euyuil.forefinger.meta.ViewMetaDataColumn;
import com.euyuil.forefinger.serde.ArrayDataRow;
import com.euyuil.forefinger.serde.DataRow;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Just project something with some filter.
 *
 * Input view could be a very SimpleViewMetaData.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.27
 */
public abstract class SimpleViewMapReduce extends ViewMapReduce {

    public static class ViewMapper extends ViewMapReduce.ViewMapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            if (viewMetaDataKeyUsage != ViewMetaData.KeyUsage.SIMPLE)
                throw new IOException("Expected simple view");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

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

    public static class ViewReducer extends ViewMapReduce.ViewReducer<LongWritable, Text, NullWritable, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            if (viewMetaDataKeyUsage != ViewMetaData.KeyUsage.SIMPLE)
                throw new IOException("Expected simple view");
        }

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text text : values) // TODO Use the serializer and deserializer.
                context.write(NullWritable.get(), text);
        }
    }
}
