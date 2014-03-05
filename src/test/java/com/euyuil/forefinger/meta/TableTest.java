package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.XStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public class TableTest {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testSerializeAndDeserialize() {

        Table table = new Table();

        table.setName("user");

        table.setPaths(new String[] {
                "/opt/forefinger/user-000.txt",
                "/opt/forefinger/user-001.txt",
        });

        TableSchema schema = new TableSchema();

        schema.setTableColumns(new TableColumn[] {
                new TableColumn("id", Long.class),
                new TableColumn("name", String.class),
                new TableColumn("age", Double.class),
                new TableColumn("description", String.class),
        });

        schema.setTableIndices(new TableIndex[] {
                new TableIndex(TableIndex.Type.PRIMARY, "id"),
                new TableIndex(TableIndex.Type.UNIQUE, "name"),
                new TableIndex(TableIndex.Type.INDEX, "age"),
                new TableIndex(TableIndex.Type.FULLTEXT, "description"),
        });

        table.setSchema(schema);

        XStream xStream = new XStream();
        xStream.autodetectAnnotations(true);

        String xmlText = xStream.toXML(table);
        Table sameTable = (Table) xStream.fromXML(xmlText);
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
