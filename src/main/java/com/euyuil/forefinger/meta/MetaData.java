package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

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

    protected abstract XStream getXmlSerDe();

    public String toXml() {
        return getXmlSerDe().toXML(this);
    }

    public void toXmlFile(File file)
            throws IOException {
        FileOutputStream outputStream = new FileOutputStream(file);
        getXmlSerDe().toXML(this, outputStream);
        outputStream.flush();
        outputStream.close();
    }
}
