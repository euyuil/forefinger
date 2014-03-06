package com.euyuil.forefinger;

import java.io.File;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.06
 */
public class Environment {

    private static Environment environment;

    /**
     * Gets the default environment.
     * @return The singleton Environment object.
     */
    public static Environment getDefault() {
        if (environment == null) {
            environment = new Environment();
        }
        return environment;
    }

    private String homePath;
    private String metaDataPath;

    /**
     * Constructs a default Environment.
     */
    public Environment() {
        this.homePath = System.getenv().get("FOREFINGER_HOME");
        initialize();
    }

    /**
     * Constructs an Environment with home path.
     * @param homePath The root path of the environment.
     */
    public Environment(String homePath) {
        this.homePath = homePath;
        initialize();
    }

    public String getHomePath() {
        return homePath;
    }

    public String getMetaDataPath() {
        return metaDataPath;
    }

    private void initialize() {
        metaDataPath = homePath + File.pathSeparator + "meta";
        new File(metaDataPath).mkdirs();
    }
}
