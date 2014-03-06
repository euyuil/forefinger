package com.euyuil.forefinger.meta;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
@XStreamAlias("view")
public class MetaView extends MetaData {

    static final XStream xmlSerDe = new XStream();

    static {
        xmlSerDe.processAnnotations(new Class[]{
                MetaData.class,
                MetaDataColumn.class,
                MetaView.class,
                MetaViewColumn.class,
        });
    }

    public static MetaView fromXml(String xml) {
        return (MetaView) xmlSerDe.fromXML(xml);
    }

    public static String toXml(MetaView view) {
        return xmlSerDe.toXML(view);
    }

    @XStreamImplicit(itemFieldName = "column")
    private MetaViewColumn[] columns;

    public MetaViewColumn[] getViewColumns() {
        return columns;
    }

    public void setViewColumns(MetaViewColumn[] columns) {
        this.columns = columns;
    }

    @Override
    public MetaDataColumn[] getDataColumns() {
        return columns;
    }
}
