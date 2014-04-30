package com.euyuil.forefinger.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;

import java.io.IOException;

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

}
