package com.euyuil.forefinger.query;

import com.euyuil.forefinger.Query;
import com.euyuil.forefinger.meta.MetaDataSet;
import com.euyuil.forefinger.meta.TableMetaData;
import junit.framework.TestCase;
import org.junit.Test;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.07
 */
public class TableQueryTest extends TestCase {

    @Test
    public void testQueryConstruction() {

        TableMetaData table = MetaDataSet.getDefault().getTableMetaData("user");

        Query query = Query
                .select(table.allColumns())
                .where(
                        table.getTableColumn("id").isEqualTo(1)
                                .or(table.getTableColumn("id").isEqualTo(2))
                );

        query.execute();
    }
}
