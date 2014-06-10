package com.euyuil.forefinger.meta.condition;

import com.euyuil.forefinger.meta.MetaDataColumn;
import com.euyuil.forefinger.serde.DataRow;

/**
 * Created by Liu Yue on 2014/05/24.
 */
public class BetweenCondition extends Condition {

    private String leftBound;

    private String rightBound;

    private int columnId;

    public BetweenCondition(String leftBound, String rightBound, MetaDataColumn column) {
    }

    @Override
    public boolean fulfilled(DataRow dataRow) {
        return false;
    }

    public String getLeftBound() {
        return leftBound;
    }

    public void setLeftBound(String leftBound) {
        this.leftBound = leftBound;
    }

    public String getRightBound() {
        return rightBound;
    }

    public void setRightBound(String rightBound) {
        this.rightBound = rightBound;
    }

    public int getColumnId() {
        return columnId;
    }

    public void setColumnId(int columnId) {
        this.columnId = columnId;
    }
}
