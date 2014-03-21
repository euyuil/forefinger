package com.euyuil.forefinger;

import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.meta.TableMetaData;
import com.euyuil.forefinger.meta.TableMetaDataColumn;
import com.euyuil.forefinger.meta.TableMetaDataIndex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.06
 */
public class MetaDataSetTest {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testPutAndGetTableMetaData() throws Exception {

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

        MetaDataSet.getDefault().putMetaData(table);

        MetaDataSet testMetaDataSet = new MetaDataSet();
        TableMetaData sameTable = testMetaDataSet.getMetaData("user", TableMetaData.class);

        assert sameTable != null;

        String xml = table.toXml();
        String sameXml = sameTable.toXml();

        assert xml.equals(sameXml);
    }

    @Test
    public void testGetMetaData() throws Exception {

    }

    @Test
    public void testGetTableMetaData() throws Exception {

    }

    @Test
    public void testGetViewMetaData() throws Exception {

    }

    @Test
    public void testPutMetaData() throws Exception {

    }
}
