package com.euyuil.forefinger;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * The forefingerConfig of the Forefinger system.
 * Users can call ForefingerConfig.getDefault() to obtain a default
 * forefingerConfig object, with the default home path of Forefinger.
 *
 * To override this behavior, use the constructor with an Environment
 * object parameter.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.09
 */
@XStreamAlias("config")
public class ForefingerConfig {

    /**
     * Singleton default configuration.
     */
    public static String CONFIG_FILE_NAME = "config.xml";

    private static File getConfigFile(Environment environment) {
        File configFile = new File(environment.getHomePath() + File.separator + CONFIG_FILE_NAME);
        return configFile;
    }

    private static ForefingerConfig forefingerConfig;

    public static ForefingerConfig getDefault() {
        if (forefingerConfig == null) {
            forefingerConfig = load(Environment.getDefault());
        }
        return forefingerConfig;
    }

    /**
     * Loads a different configuration from a custom environment.
     * @param environment the custom environment.
     * @return the configuration.
     */
    public static ForefingerConfig load(Environment environment) {
        ForefingerConfig config;
        File configFile = getConfigFile(environment);
        if (configFile.exists()) {
            try {
                config = (ForefingerConfig) getXmlSerDe().fromXML(configFile);
            } catch (Exception e) {
                e.printStackTrace();
                config = new ForefingerConfig();
            }
        } else {
            config = new ForefingerConfig();
        }
        config.environment = environment;
        return config;
    }

    /**
     * Saves the configuration to the environment.
     */
    public void save() throws IOException {
        File configFile = getConfigFile(environment);
        OutputStream outputStream = new FileOutputStream(configFile);
        getXmlSerDe().toXML(this, outputStream);
        outputStream.flush();
        outputStream.close();
    }

    public ForefingerConfig() {
    }

    public ForefingerConfig(Environment environment) {
        this.environment = environment;
    }

    /**
     * The environment of this configuration.
     */
    @XStreamOmitField
    private Environment environment;

    public Environment getEnvironment() {
        return environment;
    }

    /**
     * The replication level of meta data.
     */
    private ReplicationLevel metaDataReplicationLevel = ReplicationLevel.HADOOP_FILE_SYSTEM;

    public ReplicationLevel getMetaDataReplicationLevel() {
        return metaDataReplicationLevel;
    }

    public void setMetaDataReplicationLevel(ReplicationLevel metaDataReplicationLevel) {
        this.metaDataReplicationLevel = metaDataReplicationLevel;
    }

    /**
     * The replication level of index data.
     */
    private ReplicationLevel indexDataReplicationLevel = ReplicationLevel.HADOOP_FILE_SYSTEM;

    public ReplicationLevel getIndexDataReplicationLevel() {
        return indexDataReplicationLevel;
    }

    public void setIndexDataReplicationLevel(ReplicationLevel indexDataReplicationLevel) {
        this.indexDataReplicationLevel = indexDataReplicationLevel;
    }

    /**
     * The path to the home of Forefinger in HDFS.
     */
    private String hdfsHomePath = "/opt/forefinger";

    public String getHdfsHomePath() {
        return this.hdfsHomePath;
    }

    public void setHdfsHomePath(String hdfsHomePath) {
        this.hdfsHomePath = hdfsHomePath;
    }

    /**
     * The replication level definition.
     */
    public static enum ReplicationLevel {
        HADOOP_FILE_SYSTEM, // Data will be stored in HDFS.
        LOCAL_FILE_SYSTEM_ON_ALL_NODES, // Each node keeps a copy of the data.
        LOCAL_FILE_SYSTEM_ON_NAME_NODE, // Data will be only stored in name node.
    }

    /**
     * Serialization and deserialization.
     */
    private static XStream xmlSerDe;

    private static XStream getXmlSerDe() {
        if (xmlSerDe == null) {
            xmlSerDe = new XStream();
            xmlSerDe.processAnnotations(new Class[]{
                    ForefingerConfig.class
            });
        }
        return xmlSerDe;
    }
}
