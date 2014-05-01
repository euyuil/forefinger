package com.euyuil.forefinger.utils;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.fs.Path;

/**
 * @author Liu Yue
 * @version 0.0.2014.05.01
 */
public class IndexUtils {

    private static final String INDEX_PATH_PREFIX = "/opt/forefinger/index";

    public static Path getIndexPathForSource(Path sourcePath) {
        return new Path(INDEX_PATH_PREFIX + '/' + DigestUtils.md5Hex(sourcePath.toString()));
    }
}
