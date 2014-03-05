package com.euyuil.forefinger.query;

/**
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public class Query {

    public Query where(Condition condition) {
        return this;
    }

    public Query and(Condition condition) {
        return this;
    }

    public Query or(Condition condition) {
        return this;
    }
}
