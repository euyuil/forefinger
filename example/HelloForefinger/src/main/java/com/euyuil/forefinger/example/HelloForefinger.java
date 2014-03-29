package com.euyuil.forefinger.example;

import com.euyuil.forefinger.mapreduce.SimpleViewMapReduce;
import com.euyuil.forefinger.mapreduce.ViewMapReduce;
import com.euyuil.forefinger.meta.*;
import com.euyuil.forefinger.meta.view.ViewMetaData;
import com.euyuil.forefinger.meta.view.ViewMetaDataColumn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.24
 */
public class HelloForefinger {

    private static final String TABLE_NAME = "user";
    private static final String VIEW_NAME = "userView";

    private static MetaDataSet metaDataSet;

    private static TableMetaData createTable() {

        TableMetaData table = new TableMetaData(metaDataSet);

        table.setName(TABLE_NAME);

        table.setSources(new ArrayList<String>(Arrays.asList(
                "/opt/forefinger/user-in"
        )));

        table.setMetaDataColumns(new ArrayList<MetaDataColumn>(Arrays.asList(
                new TableMetaDataColumn(table, "id", Long.class),
                new TableMetaDataColumn(table, "name", String.class),
                new TableMetaDataColumn(table, "age", Double.class),
                new TableMetaDataColumn(table, "description", String.class)
        )));

        table.setIndices(new ArrayList<TableMetaDataIndex>(Arrays.asList(
                new TableMetaDataIndex(TableMetaDataIndex.Type.PRIMARY, "id"),
                new TableMetaDataIndex(TableMetaDataIndex.Type.UNIQUE, "name"),
                new TableMetaDataIndex(TableMetaDataIndex.Type.INDEX, "age"),
                new TableMetaDataIndex(TableMetaDataIndex.Type.FULLTEXT, "description")
        )));

        return table;
    }

    private static ViewMetaData createView() {

        ViewMetaData view = new ViewMetaData(metaDataSet);

        view.setName(VIEW_NAME);

        view.setKeyUsage(ViewMetaData.KeyUsage.SIMPLE);

        view.setSources(new ArrayList<MetaData>(Arrays.asList(
                metaDataSet.getMetaData(TABLE_NAME)
        )));

        view.setMetaDataColumns(new ArrayList<MetaDataColumn>(Arrays.asList(
                new ViewMetaDataColumn(view, "userName", "name"),
                new ViewMetaDataColumn(view, "userAge", "age")
        )));

        return view;
    }

    private static void insertIntoTable() {

        String rows1 = "1,a,10,aa\n2,b,11,bb\n3,c,12,cc";
        String rows2 = "4,d,13,dd\n5,e,14,ee\n6,f,15,ff";

        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            OutputStream outputStream1 = fileSystem.create(new Path("/opt/forefinger/user-in/part-000.txt"));
            OutputStream outputStream2 = fileSystem.create(new Path("/opt/forefinger/user-in/part-001.txt"));
            PrintWriter printWriter1 = new PrintWriter(outputStream1);
            PrintWriter printWriter2 = new PrintWriter(outputStream2);
            printWriter1.print(rows1);
            printWriter2.print(rows2);
            printWriter1.flush();
            printWriter2.flush();
            printWriter1.close();
            printWriter2.close();
            outputStream1.close();
            outputStream2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String ... args) throws Exception {

        metaDataSet = MetaDataSet.getDefault();

        System.out.println("Creating test table and view...");
        metaDataSet.putMetaData(createTable());
        metaDataSet.putMetaData(createView());

        System.out.println("Inserting test data...");
        insertIntoTable();

        ViewMetaData viewMetaData = metaDataSet.getMetaData(VIEW_NAME, ViewMetaData.class);
        TableMetaData sourceMetaData = (TableMetaData) viewMetaData.getSource();

        System.out.println("ViewMetaData:");
        System.out.println(viewMetaData.toXml());
        System.out.println();

        System.out.println("TableMetaData:");
        System.out.println(sourceMetaData.toXml());
        System.out.println();

        Configuration configuration = new Configuration();

        configuration.set(ViewMapReduce.PARAM_VIEW_NAME, VIEW_NAME);

        Job job = new Job(configuration, "testJob");

        job.setJarByClass(ViewMapReduce.class);

        job.setMapperClass(SimpleViewMapReduce.ViewMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(SimpleViewMapReduce.ViewReducer.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        for (String source : sourceMetaData.getSources())
            FileInputFormat.addInputPath(job, new Path(source));

        FileSystem.get(configuration).delete(new Path("/opt/forefinger/user-out"), true);
        FileOutputFormat.setOutputPath(job, new Path("/opt/forefinger/user-out"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
