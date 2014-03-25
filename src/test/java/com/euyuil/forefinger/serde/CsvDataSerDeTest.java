package com.euyuil.forefinger.serde;

import com.euyuil.forefinger.Environment;
import com.euyuil.forefinger.ForefingerConfig;
import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.meta.TableMetaData;
import com.euyuil.forefinger.meta.ViewMetaData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.25
 */
public class CsvDataSerDeTest {

    private ForefingerConfig config;

    private MetaDataSet metaDataSet;

    TableMetaData tableMetaData;
    ViewMetaData viewMetaData;

    CsvDataSerDe csvDataSerDe;

    @Before
    public void setUp() throws Exception {

        config = new ForefingerConfig(Environment.getDefault());
        config.setMetaDataReplicationLevel(ForefingerConfig.ReplicationLevel.LOCAL_FILE_SYSTEM_ON_ALL_NODES);
        metaDataSet = new MetaDataSet(config);

        tableMetaData = metaDataSet.getMetaData("user", TableMetaData.class);
        viewMetaData = metaDataSet.getMetaData("userView", ViewMetaData.class);

        csvDataSerDe = new CsvDataSerDe();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testDeserializeAndSerialize() {

        csvDataSerDe.setMetaData(tableMetaData);

        String[] rows = new String[]{
                " 3,   \"fd fd ds\\\" fd f, \"  , 11 , fd  ",
                "1,Yue Liu,22.6,Author of this project",
                "2,\"Some Quote Guy\",10,sf fds fds ",
        };

        for (String row : rows) {
            DataRow dataRow = csvDataSerDe.deserialize(row);
            String serializedRow = csvDataSerDe.serialize(dataRow);
            DataRow deserializedRow = csvDataSerDe.deserialize(serializedRow);
            String sameSerializedRow = csvDataSerDe.serialize(deserializedRow);
            assert serializedRow.equals(sameSerializedRow);
        }
    }
}
