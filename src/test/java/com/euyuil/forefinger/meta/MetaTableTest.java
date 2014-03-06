package com.euyuil.forefinger.meta;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public class MetaTableTest {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testSerializeAndDeserialize() {

        MetaTable table = new MetaTable();

        table.setName("user");

        table.setPaths(new String[] {
                "/opt/forefinger/user-000.txt",
                "/opt/forefinger/user-001.txt",
        });

        table.setTableColumns(new MetaTableColumn[]{
                new MetaTableColumn("id", Long.class),
                new MetaTableColumn("name", String.class),
                new MetaTableColumn("age", Double.class),
                new MetaTableColumn("description", String.class),
        });

        table.setTableIndices(new MetaTableIndex[]{
                new MetaTableIndex(MetaTableIndex.Type.PRIMARY, "id"),
                new MetaTableIndex(MetaTableIndex.Type.UNIQUE, "name"),
                new MetaTableIndex(MetaTableIndex.Type.INDEX, "age"),
                new MetaTableIndex(MetaTableIndex.Type.FULLTEXT, "description"),
        });

        String xml = MetaTable.toXml(table);
        MetaTable sameTable = MetaTable.fromXml(xml);
        String sameXml = MetaTable.toXml(sameTable);

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
