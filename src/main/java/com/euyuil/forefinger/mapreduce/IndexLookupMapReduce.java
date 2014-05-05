package com.euyuil.forefinger.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;

import java.io.IOException;

/**
 * Simple index look up MapReduce job.
 * This will go through all the content of the index.
 * Line: keyword pos1,pos2,...
 * Map from (keyword) to (keyword,
 *
 * @author Liu Yue
 * @version 0.0.2014.04.11
 */
public class IndexLookupMapReduce extends ForefingerMapReduce {

    public static class IndexLookupMapper
            extends ForefingerMapper<LongWritable, Text, Text, Text> {

        public static final String PARAM_KEYWORD_BEGIN = "forefinger.keyword.begin";

        public static final String PARAM_KEYWORD_END = "forefinger.keyword.end";

        private String keywordBegin;

        private String keywordEnd;

        private Text text = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            keywordBegin = context.getConfiguration().get(PARAM_KEYWORD_BEGIN);
            keywordEnd = context.getConfiguration().get(PARAM_KEYWORD_END);
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
            }
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
