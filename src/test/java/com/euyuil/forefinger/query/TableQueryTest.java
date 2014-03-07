package com.euyuil.forefinger.query;

import com.euyuil.forefinger.meta.MetaDataColumn;
import com.euyuil.forefinger.meta.TableMetaDataColumn;
import junit.framework.TestCase;
import org.junit.Test;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.07
 */
public class TableQueryTest extends TestCase {

    @Test
    public void testQueryConstruction() {
        // TODO Fix the query.
        Query query = new Query()
                .where(new TableMetaDataColumn().isEqualTo(1))
                .and(new TableMetaDataColumn().isEqualTo(2));
    }
}
