package com.euyuil.forefinger.query;

/**
 * Interestingly, this class knows some of its sub classes.
 *
 * @author Liu Yue
 * @version 0.0.2014.03.05
 */
public abstract class Condition {

    public static NotCondition not(Condition condition) {
        return condition.negate();
    }

    public abstract boolean fulfilled(DataRow dataRow);

    public AndCondition and(Condition condition) {
        return new AndCondition(this, condition);
    }

    public OrCondition or(Condition condition) {
        return new OrCondition(this, condition);
    }

    public NotCondition negate() {
        return new NotCondition(this);
    }
}
