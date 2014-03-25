package com.euyuil.forefinger.serde;

import com.euyuil.forefinger.meta.MetaData;

/**
 * The default SerDe for views.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public class CsvDataSerDe implements DataSerDe {

    private MetaData metaData;

    @Override
    public void setMetaData(MetaData metaData) {
        this.metaData = metaData;
    }

    @Override
    public DataRow deserialize(String serializedDataRow) {

        ArrayDataRow dataRow = new ArrayDataRow(metaData.getMetaDataColumns().size());

        int begin = 0, end;

        for (int i = 0; i < dataRow.size(); ++i) {

            boolean canQuote = true;
            boolean insideQuote = false;
            StringBuilder sb = new StringBuilder();

            for (end = begin ; end < serializedDataRow.length(); ++end) {

                if (insideQuote) {

                    if (serializedDataRow.charAt(end) == '\\')
                        ++end;
                    else if (serializedDataRow.charAt(end) == '\"')
                        break;

                    sb.append(serializedDataRow.charAt(end));

                } else {

                    if (serializedDataRow.charAt(end) == ',')
                        break;

                    if (canQuote) {

                        if (serializedDataRow.charAt(end) == '\"') {

                            if (sb.length() > 0)
                                sb = new StringBuilder();

                            begin = end + 1;

                            insideQuote = true;

                            continue;

                        } else if (serializedDataRow.charAt(end) != ' ') {
                            canQuote = false;
                        }

                        sb.append(serializedDataRow.charAt(end));

                    } else {
                        sb.append(serializedDataRow.charAt(end));
                    }

                }
            }

            Class type = metaData.getMetaDataColumns().get(i).getResultType();
            String columnString = sb.toString();

            if (type.equals(Integer.class))
                dataRow.set(i, Integer.parseInt(columnString.trim()));
            else if (type.equals(String.class))
                dataRow.set(i, columnString);
            else if (type.equals(Long.class))
                dataRow.set(i, Long.parseLong(columnString.trim()));
            else if (type.equals(Double.class))
                dataRow.set(i, Double.parseDouble(columnString.trim()));
            else if (type.equals(Float.class))
                dataRow.set(i, Float.parseFloat(columnString.trim()));
            else throw new IllegalArgumentException("Type is not supported");

            begin = end;

            if (begin >= serializedDataRow.length())
                break;

            while (begin < serializedDataRow.length() && serializedDataRow.charAt(begin) != ',')
                ++begin;

            if (++begin >= serializedDataRow.length())
                break;
        }

        return dataRow;
    }

    @Override
    public String serialize(DataRow dataRow) {

        StringBuilder sb = new StringBuilder();

        boolean isFirst = true;

        for (Object column : dataRow) {

            if (isFirst)
                isFirst = false;
            else
                sb.append(',');

            String columnString = column.toString();

            if (columnString.indexOf('\\') >= 0 || columnString.indexOf('\"') >= 0) {
                sb.append('\"');
                sb.append(columnString.replaceAll("([\\\\\"])", "\\\\$1"));
                sb.append('\"');
            } else
                sb.append(columnString);
        }

        return sb.toString();
    }
}
