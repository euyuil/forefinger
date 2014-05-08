package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.utils.IndexUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Simple index look up MapReduce job.
 * Only 1 input file is allowed.
 * This will go through all the content of the index.
 * Line: keyword pos1,pos2,...
 * Map from (keyword) to (aligned position, positions)
 *
 * @author Liu Yue
 * @version 0.0.2014.04.11
 */
public class IndexLookupMapReduce extends ForefingerMapReduce {

    public static final String PARAM_KEYWORD_BEGIN = "forefinger.keyword.begin";

    public static final String PARAM_KEYWORD_END = "forefinger.keyword.end";

    public static final String PARAM_DATA_SOURCE_PATH = "forefinger.data.path";

    public static boolean startAndWaitForIndexLookupJob(
            String tableName,
            String columnName,
            String dataSourcePath,
            String outputPath,
            String keywordBegin,
            String keywordEnd)
            throws
            IOException,
            ClassNotFoundException,
            InterruptedException {

        Path indexPath = IndexUtils.getIndexPath(tableName, columnName, dataSourcePath);

        Configuration configuration = new Configuration();

        Job job = new Job(configuration, "IndexLookupMapReduce");

        job.setJarByClass(IndexLookupMapReduce.class);

        job.setMapperClass(IndexLookupMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(IndexLookupReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, indexPath);

        FileSystem.get(configuration).delete(new Path(outputPath), true);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        configuration.set(PARAM_DATA_SOURCE_PATH, dataSourcePath);
        configuration.set(PARAM_KEYWORD_BEGIN, keywordBegin);
        configuration.set(PARAM_KEYWORD_END, keywordEnd);

        return job.waitForCompletion(true);
    }

    public static class IndexLookupMapper
            extends ForefingerMapper<LongWritable, Text, LongWritable, LongWritable> {

        private String keywordBegin;

        private String keywordEnd;

        private Path dataSourcePath;

        private long dataSourceBlockSize;

        private LongWritable alignedPosition = new LongWritable();

        private LongWritable positionWritable = new LongWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            keywordBegin = context.getConfiguration().get(PARAM_KEYWORD_BEGIN);
            keywordEnd = context.getConfiguration().get(PARAM_KEYWORD_END);
            String pathStr = context.getConfiguration().get(PARAM_DATA_SOURCE_PATH);
            dataSourcePath = new Path(pathStr);
            dataSourceBlockSize = dataSourcePath.getFileSystem(
                    context.getConfiguration()).getDefaultBlockSize(dataSourcePath);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int indexOfTab = line.indexOf('\t');
            String keyword = line.substring(0, indexOfTab);
            int compareBegin = keyword.compareTo(keywordBegin);
            int compareEnd = keyword.compareTo(keywordEnd);
            if (compareBegin >= 0 && compareEnd < 0) {

                // Extract all the positions.
                String positions = line.substring(indexOfTab + 1);

                if (positions.length() == 0)
                    return; // make sure there's at least 1 number

                int indexOfComma = -1, startPos;
                do {
                    startPos = indexOfComma + 1;
                    indexOfComma = positions.indexOf(',', startPos);

                    String numberStr;
                    if (indexOfComma == -1)
                        numberStr = positions.substring(startPos);
                    else
                        numberStr = positions.substring(startPos, indexOfComma);
                    long pos = Long.valueOf(numberStr);

                    alignedPosition.set((pos / dataSourceBlockSize) * dataSourceBlockSize);
                    positionWritable.set(pos);
                    context.write(alignedPosition, positionWritable);

                } while (indexOfComma != -1);
            }
        }
    }

    public static class IndexLookupReducer
            extends ForefingerReducer<LongWritable, LongWritable, NullWritable, LongWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(NullWritable.get(), key);
        }
    }

    public static class MyRecordReader extends KeyValueLineRecordReader {

        private boolean firstKeyValueRead = false;

        public MyRecordReader(Configuration conf) throws IOException {
            super(conf);
        }

        @Override
        public synchronized boolean nextKeyValue() throws IOException {
            if (firstKeyValueRead)
                return false;
            firstKeyValueRead = true;
            return super.nextKeyValue();
        }
    }
}
