package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.meta.ViewMetaData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.19
 */
public class SimpleViewMapReduce {

    public static class SimpleViewMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private ViewMetaData viewMetaData;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // TODO Read viewMetaData from job configuration.
            if (!viewMetaData.isSimple()) {
                throw new IOException("The view is not simple view.");
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
}