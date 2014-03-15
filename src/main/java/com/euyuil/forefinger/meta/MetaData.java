package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public abstract class MetaData {

    /**
     * The name of the table.
     */
    @XStreamAsAttribute
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public abstract MetaDataColumn[] getDataColumns();

    public abstract MetaDataColumn getDataColumn(String columnName);

    protected abstract XStream getXmlSerDe();

    public String toXml() {
        return getXmlSerDe().toXML(this);
    }

    public void toXmlFile(File file) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(file);
        toXmlStream(outputStream);
    }

    public void toXmlStream(OutputStream outputStream) throws IOException {
        getXmlSerDe().toXML(this, outputStream);
        outputStream.flush();
        outputStream.close();
    }
}
