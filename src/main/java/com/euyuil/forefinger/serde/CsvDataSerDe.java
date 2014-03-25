package com.euyuil.forefinger.serde;

import com.euyuil.forefinger.meta.MetaData;

/**
 * Not only comma separated values. It's characters separated values.
 * The default SerDe for views.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public class CsvDataSerDe implements DataSerDe {

    private char separator = ',';

    private char escapeChar = '\\';

    private char quoteChar = '"';

    private String 

    private MetaData metaData;

    public char getSeparator() {
        return separator;
    }

    public void setSeparator(char separator) {
        this.separator = separator;
    }

    @Override
    public void setMetaData(MetaData metaData) {
        this.metaData = metaData;
    }

    @Override
    public DataRow deserialize(String serializedDataRow) {

        ArrayDataRow dataRow = new ArrayDataRow(metaData.getMetaDataColumns().size());

        int index = 0;
        for (int i = 0; i < dataRow.size(); ++i) {
            for ( ; index < serializedDataRow.length(); ++index) {
                if (serializedDataRow.charAt(index) == separator)
                    break;
                if (serializedDataRow.charAt(index) == '\\')
                    ++index;
            }
        }

        return dataRow;
    }

    @Override
    public String serialize(DataRow dataRow) {

        StringBuilder sb = new StringBuilder();

        boolean isFirst = true;

        for (Object column : dataRow) {

            String columnString = column.toString();
            sb.append(columnString.replace("\"", "\\\""));

            if (isFirst)
                isFirst = false;
            else
                sb.append(separator);
        }

        return sb.toString();
    }
}
