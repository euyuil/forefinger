package com.euyuil.forefinger;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.09
 */
public class Configuration {

    private DataReplication tableSchemaReplication = DataReplication.HDFS;

    private DataReplication viewSchemaReplication = DataReplication.HDFS;

    private DataReplication indexReplication = DataReplication.HDFS;

    private String hdfsHomePath = "/opt/forefinger";

    // Local file system path should be specified by
    // the environment variable FOREFINGER_HOME
    // and obtained by Environment.getDefault().getHomePath().
    // private String localFileSystemHomePath;

    public static enum DataReplication {
        HDFS,
        NAME_NODE,
        ALL_NODES,
    }
}
