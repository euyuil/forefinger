package com.euyuil.forefinger.example;

import com.euyuil.forefinger.mapreduce.CompositeKeyWritable;
import com.euyuil.forefinger.mapreduce.CompositeValueWritable;
import com.euyuil.forefinger.mapreduce.JoinViewMapReduce;
import com.euyuil.forefinger.mapreduce.ViewMapReduce;
import com.euyuil.forefinger.meta.*;
import com.euyuil.forefinger.meta.view.JoinViewMetaData;
import com.euyuil.forefinger.meta.view.ViewMetaDataColumn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class JoinViewExample {

    private static final String TABLE_NAME = "user";
    private static final String TABLE2_NAME = "user2";
    private static final String JOIN_VIEW_NAME = "heterogeneous";

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

    private static TableMetaData createTable2() {

        TableMetaData table = new TableMetaData(metaDataSet);

        table.setName(TABLE2_NAME);

        table.setSources(new ArrayList<String>(Arrays.asList(
                "/opt/forefinger/user2-in"
        )));

        table.setMetaDataColumns(new ArrayList<MetaDataColumn>(Arrays.asList(
                new TableMetaDataColumn(table, "userName", String.class),
                new TableMetaDataColumn(table, "userQuote", String.class)
        )));

        table.setIndices(new ArrayList<TableMetaDataIndex>(Arrays.asList(
                new TableMetaDataIndex(TableMetaDataIndex.Type.UNIQUE, "userName"),
                new TableMetaDataIndex(TableMetaDataIndex.Type.INDEX, "userQuote")
        )));

        return table;
    }

    private static JoinViewMetaData createJoinView() {

        JoinViewMetaData view = new JoinViewMetaData(metaDataSet);

        view.setName(JOIN_VIEW_NAME);

        view.setSources(new ArrayList<JoinViewMetaData.JoinSource>(Arrays.asList(
                new JoinViewMetaData.JoinSource(TABLE_NAME, JoinViewMetaData.JoinType.OUTER),
                new JoinViewMetaData.JoinSource(TABLE2_NAME, JoinViewMetaData.JoinType.OUTER)
        )));

        view.setMetaDataColumns(new ArrayList<MetaDataColumn>(Arrays.asList(
                new ViewMetaDataColumn(view, "name", TABLE_NAME, "name"),
                new ViewMetaDataColumn(view, "description", TABLE_NAME, "description"),
                new ViewMetaDataColumn(view, "quote", TABLE2_NAME, "userQuote")
        )));

        view.setJoinItems(new ArrayList<JoinViewMetaData.JoinItem>(Arrays.asList(
                new JoinViewMetaData.JoinItem(new ArrayList<JoinViewMetaData.JoinItem.Column>(Arrays.asList(
                        new JoinViewMetaData.JoinItem.Column(TABLE_NAME, "name"),
                        new JoinViewMetaData.JoinItem.Column(TABLE2_NAME, "userName")
                )))
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

    private static void insertIntoTable2() {

        String rows1 = "a,alpha\ng,golf\nc,charlie";
        String rows2 = "d,delta\nh,hotel\nf,foxtrot";

        try {
            FileSystem fileSystem = FileSystem.get(new Configuration());
            OutputStream outputStream1 = fileSystem.create(new Path("/opt/forefinger/user2-in/part-000.txt"));
            OutputStream outputStream2 = fileSystem.create(new Path("/opt/forefinger/user2-in/part-001.txt"));
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
        metaDataSet.putMetaData(createTable2());
        metaDataSet.putMetaData(createJoinView());

        System.out.println("Inserting test data...");
        insertIntoTable();
        insertIntoTable2();

        JoinViewMetaData joinViewMetaData = metaDataSet.getMetaData(JOIN_VIEW_NAME, JoinViewMetaData.class);
        ArrayList<TableMetaData> tableMetaDataList = new ArrayList<TableMetaData>();
        for (JoinViewMetaData.JoinSource joinSource : joinViewMetaData.getSources())
            tableMetaDataList.add(metaDataSet.getMetaData(joinSource.getDataName(), TableMetaData.class));

        System.out.println("JoinViewMetaData:");
        System.out.println(joinViewMetaData.toXml());
        System.out.println();

        System.out.println("TableMetaData:");
        for (TableMetaData tableMetaData : tableMetaDataList)
            System.out.println(tableMetaData.toXml());
        System.out.println();

        Configuration reduceConf = new Configuration();

        reduceConf.set(JoinViewMapReduce.PARAM_DATA_NAME, JOIN_VIEW_NAME);

        Job reduceJob = new Job(reduceConf, "reduceJob");

        reduceJob.setJarByClass(ViewMapReduce.class);

        reduceJob.setReducerClass(JoinViewMapReduce.JoinViewReducer.class);
        reduceJob.setOutputKeyClass(NullWritable.class);
        reduceJob.setOutputValueClass(Text.class);

        String reduceOutputLocation = "/opt/forefinger/joinViewTestOutput";

        FileSystem.get(reduceConf).delete(new Path(reduceOutputLocation), true);
        FileOutputFormat.setOutputPath(reduceJob, new Path(reduceOutputLocation));

        // Do map jobs and configure reduce job.
        for (TableMetaData tableMetaData : tableMetaDataList) {

            Configuration mapConf = new Configuration();

            mapConf.set(JoinViewMapReduce.PARAM_DATA_NAME, JOIN_VIEW_NAME);

            Job mapJob = new Job(mapConf, "testJob");

            mapJob.setJarByClass(ViewMapReduce.class);

            mapJob.setMapperClass(JoinViewMapReduce.JoinViewMapper.class);
            mapJob.setMapOutputKeyClass(CompositeKeyWritable.class);
            mapJob.setMapOutputValueClass(CompositeValueWritable.class);

            for (String source : tableMetaData.getSources())
                FileInputFormat.addInputPath(mapJob, new Path(source));

            String mapOutputLocation = String.format("/opt/forefinger/tempJoinMapOutput/%s", tableMetaData.getName());

            FileInputFormat.addInputPath(reduceJob, new Path(mapOutputLocation));

            FileSystem.get(mapConf).delete(new Path(mapOutputLocation), true);
            FileOutputFormat.setOutputPath(mapJob, new Path(mapOutputLocation));

            boolean mapJobSucceeded = mapJob.waitForCompletion(true);

            if (!mapJobSucceeded)
                System.exit(-1);
        }

        // Do reduce job.
        System.exit(reduceJob.waitForCompletion(true) ? 0 : -1);
    }
}
