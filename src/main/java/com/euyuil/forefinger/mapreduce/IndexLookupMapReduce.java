package com.euyuil.forefinger.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.List;

/**
 * @author Liu Yue
 * @version 0.0.2014.04.11
 */
public class IndexLookupMapReduce {

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

    public static class MyTextInputFormat extends TextInputFormat {

        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
            return super.createRecordReader(split, context);
        }

        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {
            return super.getSplits(job);
        }
    }
}
