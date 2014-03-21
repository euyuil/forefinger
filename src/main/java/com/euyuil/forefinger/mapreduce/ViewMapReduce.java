package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.meta.BasicViewMetaData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.19
 */
public class ViewMapReduce {

    public static class OrderKeyFFF implements WritableComparable<OrderKeyFFF> {

        @Override
        public int compareTo(OrderKeyFFF o) {
            return 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {

        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {

        }
    }

    public static class ViewMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private BasicViewMetaData basicViewMetaData;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // TODO Read basicViewMetaData from job configuration.
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (basicViewMetaData.getKeyType() == BasicViewMetaData.KeyType.ORDER) {
                //
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
}
