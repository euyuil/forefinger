package com.euyuil.forefinger;

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

    /**
     * Constructs a default Environment.
     */
    public Environment() {
        initialize(System.getenv().get("FOREFINGER_HOME"));
    }

    /**
     * Constructs an Environment with home path.
     * @param homePath The root path of the environment.
     */
    public Environment(String homePath) {
        initialize(homePath);
    }

    public String getHomePath() {
        return homePath;
    }

    private void initialize(String homePath) {
        if (homePath == null)
            throw new IllegalArgumentException("Cannot set home path to null. " +
                    "Please check your arguments for the constructor of class " +
                    "Environment, or the environment variable FOREFINGER_HOME " +
                    "in your operating system.");
        this.homePath = homePath;
    }
}
