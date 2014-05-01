package com.euyuil.forefinger.utils;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.Path;

/**
 * @author Liu Yue
 * @version 0.0.2014.05.01
 */
public class IndexUtils {

    private static final String INDEX_PATH_PREFIX = "/opt/forefinger/index";

    public static Path getIndexPath(String tableName, String columnName, String source) {
        return new Path(String.format(
                "%s/%s-%s-%s",
                INDEX_PATH_PREFIX,
                tableName,
                columnName,
                DigestUtils.md5Hex(source)));
    }
}
