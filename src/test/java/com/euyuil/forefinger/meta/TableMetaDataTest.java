package com.euyuil.forefinger.meta;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public class TableMetaDataTest {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testSerializeAndDeserialize() {

        TableMetaData table = new TableMetaData();

        table.setName("user");

        table.setPaths(new String[] {
                "/opt/forefinger/user-000.txt",
                "/opt/forefinger/user-001.txt",
        });

        table.setTableColumns(new TableMetaDataColumn[]{
                new TableMetaDataColumn("id", Long.class),
                new TableMetaDataColumn("name", String.class),
                new TableMetaDataColumn("age", Double.class),
                new TableMetaDataColumn("description", String.class),
        });

        table.setTableIndices(new TableMetaDataIndex[]{
                new TableMetaDataIndex(TableMetaDataIndex.Type.PRIMARY, "id"),
                new TableMetaDataIndex(TableMetaDataIndex.Type.UNIQUE, "name"),
                new TableMetaDataIndex(TableMetaDataIndex.Type.INDEX, "age"),
                new TableMetaDataIndex(TableMetaDataIndex.Type.FULLTEXT, "description"),
        });

        String xml = table.toXml();
        TableMetaData sameTable = TableMetaData.fromXml(xml);
        String sameXml = sameTable.toXml();

        assert xml.trim().length() > 0;
        assert xml.equals(sameXml);
    }

    @Test
    public void testGetName() throws Exception {

    }

    @Test
    public void testSetName() throws Exception {

    }

    @Test
    public void testGetPaths() throws Exception {

    }

    @Test
    public void testSetPaths() throws Exception {

    }

    @Test
    public void testGetSchema() throws Exception {

    }

    @Test
    public void testSetSchema() throws Exception {

    }
}
