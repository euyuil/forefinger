package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.Configuration;
import com.thoughtworks.xstream.XStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the set of all the meta data, according to the
 * configuration.
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

    private static String createMetaDataDir(String homePath) {
        String metaDataDir = homePath + File.separator + "meta";
        new File(metaDataDir).mkdirs();
        return metaDataDir;
    }

    private Configuration configuration;
    private String metaDataDir;
    private String indexDir;

    private Map<String, MetaData> metaDataMap =
            new HashMap<String, MetaData>();

    private MetaDataSet() {
        this.initialize(Configuration.getDefault());
    }

    public MetaDataSet(Configuration configuration) {
        this.initialize(configuration);
    }

    private void initialize(Configuration configuration) {

        this.configuration = configuration;

        if (configuration.getMetaDataReplication() ==
                Configuration.Replication.HADOOP_FILE_SYSTEM) {
            this.metaDataDir = configuration.getHdfsHomePath() + "/meta";
        } else if (configuration.getMetaDataReplication() ==
                Configuration.Replication.LOCAL_FILE_SYSTEM_ON_ALL_NODES) {
            this.metaDataDir = configuration.getEnvironment().getHomePath() + File.separator + "meta";
            // TODO
        } else if (configuration.getMetaDataReplication() ==
                Configuration.Replication.LOCAL_FILE_SYSTEM_ON_NAME_NODE) {
            // TODO
        }

        if (configuration.getIndexReplication() ==
                Configuration.Replication.HADOOP_FILE_SYSTEM) {
            this.metaDataDir = configuration.getHdfsHomePath() + "/index";
        } else if (configuration.getMetaDataReplication() ==
                Configuration.Replication.LOCAL_FILE_SYSTEM_ON_ALL_NODES) {
            this.metaDataDir = configuration.getEnvironment().getHomePath() + File.separator + "index";
            // TODO
        } else if (configuration.getMetaDataReplication() ==
                Configuration.Replication.LOCAL_FILE_SYSTEM_ON_NAME_NODE) {
            // TODO
        }
    }

    private InputStream getInputStream(String path, Configuration.Replication replication) {
        try {
            if (replication == Configuration.Replication.HADOOP_FILE_SYSTEM) {
                FileSystem fileSystem = FileSystem.get(new org.apache.hadoop.conf.Configuration());
                if (fileSystem == null)
                    return null;
                return fileSystem.open(new Path(path));
            } else if (replication == Configuration.Replication.LOCAL_FILE_SYSTEM_ON_ALL_NODES) {
                File file = new File(path);
                if (!file.exists() || !file.isFile())
                    return null;
                return new FileInputStream(path);
            }
        } catch (IOException ioe) {
        }
        return null;
    }

    private OutputStream getOutputStream(String path, Configuration.Replication replication) {
        try {
            if (replication == Configuration.Replication.HADOOP_FILE_SYSTEM) {
                FileSystem fileSystem = FileSystem.get(new org.apache.hadoop.conf.Configuration());
                if (fileSystem == null)
                    return null;
                return fileSystem.create(new Path(path));
            } else if (replication == Configuration.Replication.LOCAL_FILE_SYSTEM_ON_ALL_NODES) {
                return new FileOutputStream(path);
            }
        } catch (IOException ioe) {
        }
        return null;
    }

    public MetaData getMetaData(String dataName) {
        if (metaDataMap.containsKey(dataName))
            return metaDataMap.get(dataName);
        String metaDataPath = metaDataDir + File.separator + dataName + ".xml";
        InputStream metaDataInputStream = getInputStream(metaDataPath, configuration.getMetaDataReplication());
        if (metaDataInputStream == null)
            return null;
        XStream xStream = new XStream();
        xStream.processAnnotations(new Class[] {
                TableMetaData.class,
                BasicViewMetaData.class,
        });
        MetaData metaData = (MetaData) xStream.fromXML(metaDataInputStream);
        metaDataMap.put(dataName, metaData);
        try {
            metaDataInputStream.close();
        } catch (IOException ioe) {
        }
        return metaData;
    }

    @SuppressWarnings("unchecked")
    public <T extends MetaData> T getMetaData(String dataName, Class<T> clazz) {
        MetaData result = getMetaData(dataName);
        if (result.getClass().equals(clazz))
            return (T) result;
        return null;
    }

    public void putMetaData(MetaData data) throws IOException {
        String metaDataPath = metaDataDir + File.separator + data.getName() + ".xml";
        OutputStream metaDataOutputStream = getOutputStream(metaDataPath, configuration.getMetaDataReplication());
        data.toXmlStream(metaDataOutputStream);
        metaDataMap.put(data.getName(), data);
    }
}
