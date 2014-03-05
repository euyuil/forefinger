package com.euyuil.forefinger.meta;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public interface DataRow {

    DataColumn[] getDataColumns();

    Object[] getValues();
}
