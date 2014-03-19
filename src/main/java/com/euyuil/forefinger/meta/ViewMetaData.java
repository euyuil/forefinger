package com.euyuil.forefinger.meta;

import com.euyuil.forefinger.meta.condition.Condition;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;

import java.io.File;
import java.util.ArrayList;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
@XStreamAlias("view")
public class ViewMetaData extends MetaData {

    /**
     * Constructs a ViewMetaData object specifying MetaDataSet object.
     * @param metaDataSet the MetaDataSet object.
     */
    public ViewMetaData(MetaDataSet metaDataSet) {
        super(metaDataSet);
    }

    @XStreamAlias("where")
    private Condition condition;

    @XStreamAlias("orderBy")
    private ArrayList<String> orders; // TODO

    @XStreamAlias("groupBy")
    private ArrayList<String> groups; // TODO

    public boolean isSimple() {
        return true; // TODO Maybe use another classes to represent compound views.
    }

    public boolean isAggregate() {
        return false; // TODO
    }

    public boolean isUnion() {
        return false; // TODO
    }

    public boolean isMaterial() {
        if (isUnion())
            return true;
        return false; // TODO
    }

    public boolean isJoin() {
        return false; // TODO
    }

    public boolean isTemporary() {
        return false; // TODO
    }

    /**
     * @return the XML SerDe.
     */
    @Override
    protected XStream getXmlSerDe() {
        return xmlSerDe;
    }

    static final XStream xmlSerDe = new XStream();

    static {
        xmlSerDe.processAnnotations(new Class[]{
                MetaData.class,
                MetaDataColumn.class,
                ViewMetaData.class,
                ViewMetaDataColumn.class,
        });
    }

    public static ViewMetaData fromXml(String xml) {
        return (ViewMetaData) xmlSerDe.fromXML(xml);
    }

    public static ViewMetaData fromXmlFile(File xmlFile) {
        return (ViewMetaData) xmlSerDe.fromXML(xmlFile);
    }
}
