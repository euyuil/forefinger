package com.euyuil.forefinger.example;

import com.euyuil.forefinger.mapreduce.IndexLookupMapReduce;
import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.meta.TableMetaData;

import java.io.IOException;
import java.util.List;

/**
 * @author Liu Yue
 * @version 0.0.2014.05.08
 */
public class IndexLookupExample {

    public static void main(String... args)
            throws InterruptedException, IOException, ClassNotFoundException {

        MetaDataSet metaDataSet = MetaDataSet.getDefault();

        TableMetaData tableMetaData = metaDataSet.getMetaData("person", TableMetaData.class);

        List<String> sources = tableMetaData.getSources();

        System.out.println("Sources:");
        for (String source : sources)
            System.out.println("\tSource: " + source);

        long beforeLookupIndex = System.currentTimeMillis();

        boolean result = IndexLookupMapReduce.startAndWaitForIndexLookupJob(
                tableMetaData.getName(),
                "id",
                sources.get(0),
                "/opt/forefinger/result.txt",
                "110000000",
                "190000000"
        );

        if (result)
            System.out.println("Successfully done");
        else
            System.out.println("failed");

        double indexLookupTime = (System.currentTimeMillis() - beforeLookupIndex) / 1000.0;

        System.out.println(String.format("Time used %f", indexLookupTime));
    }
}
