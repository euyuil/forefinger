package com.euyuil.forefinger.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

/**
 * Utilities for HDFS.
 * @author Yue Liu
 * @version 0.0.2014.04.30
 */
public class HdfsUtils {

    public static void writeStringToFile(String content, Path path) throws IOException {
        PrintWriter writer = beginWrite(path);
        writer.println(content);
        endWrite(writer);
    }

    public static PrintWriter beginWrite(Path path) throws IOException {
        FileSystem fileSystem = path.getFileSystem(new Configuration());
        OutputStream outputStream = fileSystem.create(path, true);
        return new PrintWriter(outputStream);
    }

    public static void endWrite(PrintWriter printWriter) {
        printWriter.flush();
        printWriter.close();
    }
}
