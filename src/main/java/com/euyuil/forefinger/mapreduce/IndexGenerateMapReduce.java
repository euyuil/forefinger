package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.serde.DataRow;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Generates indices of a physical data set.
 *
 * @author Liu Yue
 * @version 0.0.2014.04.10
 */
public class IndexGenerateMapReduce {

    public static String PARAM_COLUMN_TO_GENERATE_INDEX = "columnToGenerateIndex";

    public static class IndexGenerateMapper extends ViewMapReduce.ViewMapper<LongWritable, Text, Text, LongWritable> {

        private String columnToGenerateIndex;
        private int columnId;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            columnToGenerateIndex = context.getConfiguration().get(PARAM_COLUMN_TO_GENERATE_INDEX);
            columnId = viewMetaData.getMetaDataColumnIndex(columnToGenerateIndex);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            DataRow dataRow = viewMetaDataDeserializer.deserialize(value.toString());

            text.set(dataRow.get(columnId, String.class));
            context.write(text, key);
        }
    }

    public static class IndexGenerateReducer extends ViewMapReduce.ViewReducer<Text, LongWritable, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            // TODO Avoid reading all values to memory at same time.
            StringBuilder sb = new StringBuilder();

            for (LongWritable value : values) {
                if (sb.length() == 0)
                    sb.append(value.toString());
                else {
                    sb.append(',');
                    sb.append(value.toString());
                }
            }

            text.set(sb.toString());
            context.write(key, text);
        }
    }
}
