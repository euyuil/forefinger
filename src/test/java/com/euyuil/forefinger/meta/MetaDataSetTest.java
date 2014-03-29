package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.Environment;
import com.euyuil.forefinger.ForefingerConfig;
import com.euyuil.forefinger.meta.view.ViewMetaData;
import com.euyuil.forefinger.meta.view.ViewMetaDataColumn;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.06
 */
public class MetaDataSetTest {

    private ForefingerConfig config;

    private MetaDataSet metaDataSet;

    private TableMetaData generateTableMetaData() {

        TableMetaData table = new TableMetaData(metaDataSet);

        table.setName("user");

        table.setSources(new ArrayList<String>(Arrays.asList(
                "/opt/forefinger/user-000.txt",
                "/opt/forefinger/user-001.txt"
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

    private ViewMetaData generateViewMetaData() {

        ViewMetaData view = new ViewMetaData(metaDataSet);

        view.setName("userView");

        view.setKeyUsage(ViewMetaData.KeyUsage.SIMPLE);

        view.setSources(new ArrayList<MetaData>(Arrays.asList(
                metaDataSet.getMetaData("user")
        )));

        view.setMetaDataColumns(new ArrayList<MetaDataColumn>(Arrays.asList(
                new ViewMetaDataColumn(view, "userName", "name"),
                new ViewMetaDataColumn(view, "userAge", "age")
        )));

        return view;
    }

    @Before
    public void setUp() throws Exception {
        config = new ForefingerConfig(Environment.getDefault());
        config.setMetaDataReplicationLevel(ForefingerConfig.ReplicationLevel.LOCAL_FILE_SYSTEM_ON_ALL_NODES);
        metaDataSet = new MetaDataSet(config);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testTableMetaDataSerDe() throws Exception {

        TableMetaData table = generateTableMetaData();

        String xml = table.toXml();
        TableMetaData sameTable = TableMetaData.fromXml(xml);
        String sameXml = sameTable.toXml();

        assert xml.trim().length() > 0;
        assert xml.equals(sameXml);
    }

    @Test
    public void testViewMetaDataSerDe() throws Exception {

        ViewMetaData view = generateViewMetaData();

        String xml = view.toXml();
        ViewMetaData sameView = ViewMetaData.fromXml(xml);
        String sameXml = sameView.toXml();

        assert xml.trim().length() > 0;
        assert xml.equals(sameXml);
    }

    @Test
    public void testMetaDataSetPutAndGetTable() throws Exception {

        TableMetaData table = generateTableMetaData();

        metaDataSet.putMetaData(table);

        TableMetaData sameTable = metaDataSet.getMetaData("user", TableMetaData.class, false);

        assert sameTable != null;

        String xml = table.toXml();
        String sameXml = sameTable.toXml();

        assert xml.equals(sameXml);
    }

    @Test
    public void testMetaDataSetPutAndGetView() throws Exception {

        ViewMetaData view = generateViewMetaData();

        metaDataSet.putMetaData(view);

        ViewMetaData sameView = metaDataSet.getMetaData("userView", ViewMetaData.class, false);

        assert sameView != null;

        String xml = view.toXml();
        String sameXml = sameView.toXml();

        assert xml.equals(sameXml);
    }
}
