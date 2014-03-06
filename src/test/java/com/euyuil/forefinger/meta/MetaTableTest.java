package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.XStream;
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

        XStream xStream = new XStream();
        xStream.autodetectAnnotations(true);

        String xmlText = xStream.toXML(table);
        MetaTable sameTable = (MetaTable) xStream.fromXML(xmlText);
        String sameXmlText = xStream.toXML(sameTable);

        assert xmlText.trim().length() > 0;
        assert xmlText.equals(sameXmlText);
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
