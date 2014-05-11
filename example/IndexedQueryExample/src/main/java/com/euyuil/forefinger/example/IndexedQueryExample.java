package com.euyuil.forefinger.example;

import com.euyuil.forefinger.mapreduce.SimpleViewMapReduce;
import com.euyuil.forefinger.meta.MetaDataColumn;
import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.meta.TableMetaData;
import com.euyuil.forefinger.meta.view.SimpleViewMetaData;
import com.euyuil.forefinger.meta.view.ViewMetaDataColumn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Liu Yue
 * @version 0.0.2014.05.11
 */
public class IndexedQueryExample {

    public static void main(String... args) throws InterruptedException, IOException, ClassNotFoundException {

        String viewName = "personView";

        MetaDataSet metaDataSet = MetaDataSet.getDefault();

        TableMetaData tableMetaData = metaDataSet.getMetaData("person", TableMetaData.class);

        SimpleViewMetaData simpleViewMetaData = new SimpleViewMetaData(metaDataSet);

        simpleViewMetaData.setName(viewName);

        simpleViewMetaData.setSources(Arrays.asList(
                metaDataSet.getMetaData("person")
        ));

        simpleViewMetaData.setMetaDataColumns(new ArrayList<MetaDataColumn>(Arrays.asList(
                new ViewMetaDataColumn(simpleViewMetaData, "name", "name"),
                new ViewMetaDataColumn(simpleViewMetaData, "age", "age")
        )));

        boolean result = SimpleViewMapReduce.startAndWaitForSimpleViewWithIndex(
                viewName,
                tableMetaData.getSources().get(0),
                "/opt/forefinger/result.txt/part-r-00000",
                "opt/forefinger/index-result.txt");

        if (result)
            System.out.println("Successfully ran the job");
        else
            System.out.println("Failed");
    }
}
