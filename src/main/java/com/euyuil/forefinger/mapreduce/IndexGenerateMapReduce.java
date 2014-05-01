package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.meta.TableMetaData;
import com.euyuil.forefinger.serde.DataRow;
import com.euyuil.forefinger.serde.Deserializer;
import com.euyuil.forefinger.utils.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates indices of a physical data set.
 *
 * @author Liu Yue
 * @version 0.0.2014.04.10
 */
public class IndexGenerateMapReduce extends ForefingerMapReduce {

    public static String PARAM_INDEX_COLUMN = "forefinger.index.column";

    public static boolean startAndWaitForIndexGenerateJob(String tableName, String columnName)
            throws IOException {

        List<Job> jobs = new ArrayList<Job>();

        TableMetaData tableMetaData = MetaDataSet.getDefault().getMetaData(tableName, TableMetaData.class);

        for (String source : tableMetaData.getSources()) {

            Configuration conf = new Configuration();

            conf.set(PARAM_TABLE_NAME, tableName);
            conf.set(PARAM_INDEX_COLUMN, columnName);

            Job job = new Job(conf, "IndexGenerateMapper");

            job.setJarByClass(IndexGenerateMapReduce.class);

            job.setMapperClass(IndexGenerateMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setReducerClass(IndexGenerateReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(source));

            try {
                job.submit();
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                return false;
            }

            jobs.add(job);
        }

        return JobUtils.waitForCompletion(jobs);
    }

    public static class IndexGenerateMapper
            extends ForefingerMapper<LongWritable, Text, Text, LongWritable> {

        private String indexColumnName;

        private int indexColumnId;

        private Deserializer tableRowDeserializer;

        private Text text = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            indexColumnName = context.getConfiguration().get(PARAM_INDEX_COLUMN);
            indexColumnId = getTableMetaData().getMetaDataColumnIndex(indexColumnName);
            tableRowDeserializer = getTableMetaData().getDeserializer();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            DataRow dataRow = tableRowDeserializer.deserialize(value.toString());

            text.set(dataRow.get(indexColumnId, String.class));
            context.write(text, key);
        }
    }

    public static class IndexGenerateReducer
            extends ForefingerReducer<Text, LongWritable, Text, Text> {

        private Text text = new Text();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            // TODO Avoid reading all values to memory at the same time.
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
