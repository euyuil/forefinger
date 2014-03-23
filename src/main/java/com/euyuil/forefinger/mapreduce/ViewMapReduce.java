package com.euyuil.forefinger.mapreduce;

import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.meta.ViewMetaData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.19
 */
public class ViewMapReduce {

    public static String PARAM_VIEW_NAME = "viewName";

    public static class ViewMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private ViewMetaData viewMetaData;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String viewName = context.getConfiguration().get(PARAM_VIEW_NAME);
            viewMetaData = MetaDataSet.getDefault().getMetaData(viewName, ViewMetaData.class);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (viewMetaData.getKeyUsage() == ViewMetaData.KeyUsage.ORDER) {
                //
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }
}
