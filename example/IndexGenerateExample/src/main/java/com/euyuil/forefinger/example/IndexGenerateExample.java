package com.euyuil.forefinger.example;

import com.euyuil.forefinger.mapreduce.IndexGenerateMapReduce;
import com.euyuil.forefinger.meta.MetaDataColumn;
import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.meta.TableMetaData;
import com.euyuil.forefinger.meta.TableMetaDataColumn;
import com.euyuil.forefinger.utils.HdfsUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * @author Liu Yue
 * @version 0.0.2014.05.01
 */
public class IndexGenerateExample {

    private static void createExampleTable() throws IOException {

        String tableName = "person";

        TableMetaData tableMetaData = new TableMetaData(MetaDataSet.getDefault());

        tableMetaData.setName(tableName);

        tableMetaData.setSources(new ArrayList<String>(Arrays.asList(
                "/opt/forefinger/person.txt"
        )));

        tableMetaData.setMetaDataColumns(new ArrayList<MetaDataColumn>(Arrays.asList(
                new TableMetaDataColumn(tableMetaData, "name", String.class),
                new TableMetaDataColumn(tableMetaData, "age", Integer.class)
        )));

        MetaDataSet.getDefault().putMetaData(tableMetaData);

        int entryCount = 10000000;
        int startAge = 1000;
        int endAge = 10000;
        int prefixLength = 31;
        int suffixLength = 31;

        Random random = new Random();
        PrintWriter writer = HdfsUtils.beginWrite(new Path("/opt/forefinger/person.txt"));

        for (int i = 0; i < entryCount; ++i) {
            int age = random.nextInt(endAge - startAge) + startAge;
            StringBuilder name = new StringBuilder();
            for (int j = 0; j < prefixLength; ++j)
                name.append((char) (random.nextInt(26) + 'a'));
            name.append(age);
            for (int j = 0; j < suffixLength; ++j)
                name.append((char) (random.nextInt(26) + 'a'));
            writer.println(String.format("%s,%d", name.toString(), age));
        }

        HdfsUtils.endWrite(writer);
    }

    public static void main(String... args) throws IOException {

        createExampleTable();

        boolean result = IndexGenerateMapReduce.startAndWaitForIndexGenerateJob("person", "age");

        if (result)
            System.out.println("Successfully generated index for column age of table person");
        else
            System.out.println("Failed generating index for column age of table person");
    }
}
