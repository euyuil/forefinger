package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.Environment;
import com.thoughtworks.xstream.XStream;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
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

    private String metaDataDir;
    private Map<String, MetaData> metaDataMap =
            new HashMap<String, MetaData>();

    public MetaDataSet() {
        this.metaDataDir = createMetaDataDir(
                Environment.getDefault().getHomePath());
    }

    public MetaDataSet(Environment environment) {
        this.metaDataDir = createMetaDataDir(environment.getHomePath());
    }

    public MetaDataSet(String metaDataDir) {
        this.metaDataDir = metaDataDir;
    }

    public MetaData getMetaData(String dataName) {
        if (metaDataMap.containsKey(dataName))
            return metaDataMap.get(dataName);
        String metaDataPath = metaDataDir + File.separator + dataName + ".xml";
        File metaDataFile = new File(metaDataPath);
        if (!metaDataFile.exists() || !metaDataFile.isFile())
            return null;
        XStream xStream = new XStream();
        xStream.processAnnotations(new Class[] {
                TableMetaData.class,
                ViewMetaData.class,
        });
        MetaData metaData = (MetaData) xStream.fromXML(metaDataFile);
        metaDataMap.put(dataName, metaData);
        return metaData;
    }

    public TableMetaData getTableMetaData(String tableName) {
        MetaData metaData = getMetaData(tableName);
        if (metaData instanceof TableMetaData)
            return (TableMetaData) metaData;
        return null;
    }

    public ViewMetaData getViewMetaData(String viewName) {
        MetaData metaData = getMetaData(viewName);
        if (metaData instanceof ViewMetaData)
            return (ViewMetaData) metaData;
        return null;
    }

    public void putMetaData(MetaData data) throws IOException {
        String metaDataPath = metaDataDir + File.separator + data.getName() + ".xml";
        data.toXmlFile(new File(metaDataPath));
        metaDataMap.put(data.getName(), data);
    }
}
