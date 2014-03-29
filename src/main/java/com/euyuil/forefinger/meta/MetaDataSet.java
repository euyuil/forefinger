package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.ForefingerConfig;
import com.thoughtworks.xstream.XStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Represents the set of all the meta data, according to the
 * forefingerConfig.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.06
 */
public class MetaDataSet {

    private static MetaDataSet metaDataSet;

    public static MetaDataSet getDefault() {
        if (metaDataSet == null)
            metaDataSet = new MetaDataSet();
        return metaDataSet;
    }

    private ForefingerConfig forefingerConfig;
    private String metaDataDir;
    private String indexDir;

    private Map<String, MetaData> metaDataMap =
            new HashMap<String, MetaData>();

    private MetaDataSet() {
        this.initialize(ForefingerConfig.getDefault());
    }

    public MetaDataSet(ForefingerConfig forefingerConfig) {
        this.initialize(forefingerConfig);
    }

    private void initialize(ForefingerConfig forefingerConfig) {

        this.forefingerConfig = forefingerConfig;

        if (forefingerConfig.getMetaDataReplicationLevel() ==
                ForefingerConfig.ReplicationLevel.HADOOP_FILE_SYSTEM) {
            this.metaDataDir = forefingerConfig.getHdfsHomePath() + "/meta";
            // TODO Create dir.
        } else if (forefingerConfig.getMetaDataReplicationLevel() ==
                ForefingerConfig.ReplicationLevel.LOCAL_FILE_SYSTEM_ON_ALL_NODES) {
            this.metaDataDir = forefingerConfig.getEnvironment().getHomePath() + File.separator + "meta";
            new File(this.metaDataDir).mkdirs();
            // TODO
        } else if (forefingerConfig.getMetaDataReplicationLevel() ==
                ForefingerConfig.ReplicationLevel.LOCAL_FILE_SYSTEM_ON_NAME_NODE) {
            // TODO
        }

        if (forefingerConfig.getIndexDataReplicationLevel() ==
                ForefingerConfig.ReplicationLevel.HADOOP_FILE_SYSTEM) {
            this.indexDir = forefingerConfig.getHdfsHomePath() + "/index";
            // TODO Create dir.
        } else if (forefingerConfig.getMetaDataReplicationLevel() ==
                ForefingerConfig.ReplicationLevel.LOCAL_FILE_SYSTEM_ON_ALL_NODES) {
            this.indexDir = forefingerConfig.getEnvironment().getHomePath() + File.separator + "index";
            new File(this.indexDir).mkdirs();
            // TODO
        } else if (forefingerConfig.getMetaDataReplicationLevel() ==
                ForefingerConfig.ReplicationLevel.LOCAL_FILE_SYSTEM_ON_NAME_NODE) {
            // TODO
        }
    }

    private InputStream getInputStream(String path, ForefingerConfig.ReplicationLevel replicationLevel) {
        try {
            if (replicationLevel == ForefingerConfig.ReplicationLevel.HADOOP_FILE_SYSTEM) {
                FileSystem fileSystem = FileSystem.get(new org.apache.hadoop.conf.Configuration());
                if (fileSystem == null)
                    return null;
                return fileSystem.open(new Path(path));
            } else if (replicationLevel == ForefingerConfig.ReplicationLevel.LOCAL_FILE_SYSTEM_ON_ALL_NODES) {
                File file = new File(path);
                if (!file.exists() || !file.isFile())
                    return null;
                return new FileInputStream(path);
            }
        } catch (IOException ioe) {
            return null;
        }
        return null;
    }

    private String readAsString(String path, ForefingerConfig.ReplicationLevel replicationLevel) {
        InputStream inputStream = getInputStream(path, replicationLevel);
        Scanner scanner = new Scanner(inputStream).useDelimiter("\\A");
        return scanner.hasNext() ? scanner.next() : "";
    }

    private OutputStream getOutputStream(String path, ForefingerConfig.ReplicationLevel replicationLevel) throws IOException {

        if (replicationLevel == ForefingerConfig.ReplicationLevel.HADOOP_FILE_SYSTEM) {

            FileSystem fileSystem = FileSystem.get(new Configuration());

            if (fileSystem == null)
                throw new IOException("Cannot get HDFS file system object");

            return fileSystem.create(new Path(path), true);

        } else if (replicationLevel == ForefingerConfig.ReplicationLevel.LOCAL_FILE_SYSTEM_ON_ALL_NODES) {
            return new FileOutputStream(path);
        }

        throw new IOException(String.format("The replication level %s is not supported", replicationLevel));
    }

    public MetaData getMetaData(String dataName) {
        return getMetaData(dataName, true);
    }

    public MetaData getMetaData(String dataName, boolean useCache) {

        if (useCache) {
            if (metaDataMap.containsKey(dataName))
                return metaDataMap.get(dataName);
        }

        String metaDataPath = metaDataDir + File.separator + dataName + ".xml";
        String metaDataString = readAsString(metaDataPath, forefingerConfig.getMetaDataReplicationLevel()).trim();
        String metaDataType = metaDataString.substring(1, metaDataString.indexOf(' '));

        XStream xmlSerDe;
        if (metaDataType.toLowerCase().endsWith("table"))
            xmlSerDe = TableMetaData.getXmlSerDe();
        else if (metaDataType.toLowerCase().endsWith("view"))
            xmlSerDe = ViewMetaData.getXmlSerDe();
        else
            return null;

        MetaData metaData = (MetaData) xmlSerDe.fromXML(metaDataString);
        metaDataMap.put(dataName, metaData);

        metaData.setMetaDataSet(this);

        for (MetaDataColumn metaDataColumn : metaData.getMetaDataColumns())
            metaDataColumn.setMetaData(metaData);

        return metaData;
    }

    public <T extends MetaData> T getMetaData(String dataName, Class<T> clazz) {
        return getMetaData(dataName, clazz, true);
    }

    @SuppressWarnings("unchecked")
    public <T extends MetaData> T getMetaData(String dataName, Class<T> clazz, boolean useCache) {
        MetaData metaData = getMetaData(dataName, useCache);
        if (clazz.isInstance(metaData))
            return (T) metaData;
        return null;
    }

    public void putMetaData(MetaData metaData) throws IOException {

        if (metaData == null)
            throw new NullPointerException("Cannot put null MetaData, thus parameter metaData cannot be null");

        String metaDataPath = metaDataDir + File.separator + metaData.getName() + ".xml";

        OutputStream metaDataOutputStream = getOutputStream(metaDataPath, forefingerConfig.getMetaDataReplicationLevel());
        metaData.toXmlStream(metaDataOutputStream);

        metaDataMap.put(metaData.getName(), metaData);
    }
}
