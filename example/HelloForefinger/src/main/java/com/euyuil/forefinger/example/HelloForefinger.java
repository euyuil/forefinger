package com.euyuil.forefinger.example;

import com.euyuil.forefinger.mapreduce.ViewMapReduce;
import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.meta.TableMetaData;
import com.euyuil.forefinger.meta.ViewMetaData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.24
 */
public class HelloForefinger {

    public static void main(String ... args) throws Exception {

        final String viewName = "userView";
        ViewMetaData viewMetaData = MetaDataSet.getDefault().getMetaData(viewName, ViewMetaData.class);
        TableMetaData sourceMetaData = (TableMetaData) viewMetaData.getSource();

        Configuration configuration = new Configuration();

        configuration.set(ViewMapReduce.PARAM_VIEW_NAME, viewName);

        Job job = new Job(configuration, "testJob");

        job.setMapperClass(ViewMapReduce.ViewMapper.class);

        for (String source : sourceMetaData.getSources())
            FileInputFormat.addInputPath(job, new Path(source));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
