package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.meta.OrderViewMetaData;
import com.euyuil.forefinger.meta.ViewMetaData;
import com.euyuil.forefinger.meta.ViewMetaDataColumn;
import com.euyuil.forefinger.serde.ArrayDataRow;
import com.euyuil.forefinger.serde.DataRow;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.27
 */
public class OrderViewMapReduce {

    public static class ViewMapper extends ViewMapReduce.ViewMapper<LongWritable, Text, CompositeWritable, Text> {

        private List<OrderViewMetaData.OrderByItem> orderByItems;
        private int[] orderByColumnIndices;
        private CompositeWritable compositeWritable;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            if (viewMetaDataKeyUsage != ViewMetaData.KeyUsage.ORDER)
                throw new IOException("Expected order view");
            orderByItems = viewMetaData.getOrderByItems();
            if (orderByItems == null)
                throw new IOException("OrderByItems are not set");
            orderByColumnIndices = new int[orderByItems.size()];
            for (int i = 0; i < orderByItems.size(); i++) {
                OrderViewMetaData.OrderByItem orderByItem = orderByItems.get(i);
                orderByColumnIndices[i] = viewMetaData.getMetaDataColumnIndex(orderByItem.getColumnName());
            }
            compositeWritable = new CompositeWritable(orderByColumnIndices.length);
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

            // Ordering key computation.
            for (int i = 0; i < orderByColumnIndices.length; ++i) {
                Comparable columnValue = (Comparable) dataRow.get(orderByColumnIndices[i]);
                compositeWritable.setObject(i, columnValue);
                compositeWritable.setObjectOrderBy(i, orderByItems.get(i).getOrderType());
            }

            // Write results.
            String serialized = viewMetaDataSerializer.serialize(writeDataRow);
            text.set(serialized);
            context.write(compositeWritable, text);
        }
    }

    public static class ViewReducer extends ViewMapReduce.ViewReducer<CompositeWritable, Text, NullWritable, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            if (viewMetaDataKeyUsage != ViewMetaData.KeyUsage.ORDER)
                throw new IOException("Expected order view");
        }

        @Override
        protected void reduce(CompositeWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text text : values) // TODO Use the serializer and deserializer.
                context.write(NullWritable.get(), text);
        }
    }
}
