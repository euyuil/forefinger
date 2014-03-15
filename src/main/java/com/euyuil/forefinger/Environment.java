package com.euyuil.forefinger;

/**
 * The Environment object for the Forefinger system.
 * Users could call Environment.getDefault() to obtain the default
 * Environment object.
 *
 * The path of the environment could be hacked for special purposes,
 * for example, testing. In this case you should use the constructor
 * with one String parameter.
 *
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

    /**
     * Path to the Forefinger home.
     */
    private String homePath;

    /**
     * Constructs a default Environment.
     */
    private Environment() {
        initialize(System.getenv().get("FOREFINGER_HOME"));
    }

    /**
     * Constructs an Environment with home path.
     * @param homePath The root path of the environment.
     */
    public Environment(String homePath) {
        initialize(homePath);
    }

    /**
     * @return the home path of the environment.
     */
    public String getHomePath() {
        return homePath;
    }

    /**
     * Initializes an Environment object.
     * @param homePath the path to the Forefinger home.
     */
    private void initialize(String homePath) {
        if (homePath == null)
            throw new IllegalArgumentException("Cannot set home path to null. " +
                    "Please check your arguments for the constructor of class " +
                    "Environment, or the environment variable FOREFINGER_HOME " +
                    "in your operating system.");
        this.homePath = homePath;
    }
}
