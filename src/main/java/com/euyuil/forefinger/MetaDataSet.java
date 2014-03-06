package com.euyuil.forefinger;

import com.euyuil.forefinger.meta.MetaData;
import com.euyuil.forefinger.meta.TableMetaData;
import com.euyuil.forefinger.meta.ViewMetaData;
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

    private String metaDataDir;
    private Map<String, MetaData> metaDataMap =
            new HashMap<String, MetaData>();

    public MetaDataSet() {
        this.metaDataDir = Environment.getDefault().getMetaDataPath();
    }

    public MetaDataSet(Environment environment) {
        this.metaDataDir = environment.getMetaDataPath();
    }

    public MetaDataSet(String metaDataDir) {
        this.metaDataDir = metaDataDir;
    }

    public MetaData getMetaData(String dataName) {
        if (metaDataMap.containsKey(dataName))
            return metaDataMap.get(dataName);
        String metaDataPath = metaDataDir + File.pathSeparator + dataName + ".xml";
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
        String metaDataPath = metaDataDir + File.pathSeparator + data.getName() + ".xml";
        data.toXmlFile(new File(metaDataPath));
        metaDataMap.put(data.getName(), data);
    }
}
