package com.euyuil.forefinger;

/**
 * The configuration of the Forefinger system.
 * Users can call Configuration.getDefault() to obtain a default
 * configuration object, with the default home path of Forefinger.
 *
 * To override this behavior, use the constructor with an Environment
 * object parameter.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.09
 */
public class Configuration {

    private static Configuration configuration;

    public static Configuration getDefault() {
        if (configuration == null)
            configuration = new Configuration();
        return configuration;
    }

    private Environment environment;

    private Replication metaDataReplication = Replication.HADOOP_FILE_SYSTEM;

    private Replication indexReplication = Replication.HADOOP_FILE_SYSTEM;

    private String hdfsHomePath = "/opt/forefinger";

    private Configuration() {
        initialize(Environment.getDefault());
    }

    public Configuration(Environment environment) {
        initialize(environment);
    }

    public Environment getEnvironment() {
        return environment;
    }

    public Replication getMetaDataReplication() {
        return metaDataReplication;
    }

    public Replication getIndexReplication() {
        return indexReplication;
    }

    /**
     * @return the home path in HDFS
     */
    public String getHdfsHomePath() {
        return this.hdfsHomePath;
    }

    private void initialize(Environment environment) {
        this.environment = environment;
        // TODO Set the field value of this object.
    }

    public static enum Replication {
        HADOOP_FILE_SYSTEM,
        LOCAL_FILE_SYSTEM_ON_ALL_NODES,
        LOCAL_FILE_SYSTEM_ON_NAME_NODE,
    }
}
