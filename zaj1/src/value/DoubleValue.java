package value;

import java.lang.Math;

public class DoubleValue extends Value {
    private double value;

    public DoubleValue() {
        value = 0.0;
    }

    public DoubleValue(String s){
        value = Double.parseDouble(s);
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }

    @Override
    public DoubleValue add(Value myData) {
        DoubleValue result = new DoubleValue();
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        result.value = this.value + useMe;
        return result;
    }

    @Override
    public DoubleValue sub(Value myData) {
        DoubleValue result = new DoubleValue();
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        result.value = this.value - useMe;
        return result;
    }

    @Override
    public DoubleValue mul(Value myData) {
        DoubleValue result = new DoubleValue();
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        result.value = this.value * useMe;
        return result;
    }

    @Override
    public DoubleValue div(Value myData) {
        DoubleValue result = new DoubleValue();
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        result.value = this.value / useMe;
        return result;
    }

    @Override
    public DoubleValue pow(Value myData) {
        DoubleValue result = new DoubleValue();
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        result.value = Math.pow(this.value, useMe);
        return result;
    }

    @Override
    public boolean eq(Value myData) {
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        if (useMe == this.value) {
            return true;
        }
        return false;
    }

    @Override
    public boolean lte(Value myData) {
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        if (useMe >= this.value) {
            return true;
        }
        return false;
    }

    @Override
    public boolean gte(Value myData) {
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        if (useMe <= this.value) {
            return true;
        }
        return false;
    }

    @Override
    public boolean neq(Value myData) {
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        if (useMe != this.value) {
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object other) {
        return this.hashCode() == other.hashCode();
    }

    @Override
    public int hashCode() {
        int code = Double.hashCode(this.value);
        return code;
    }

    @Override
    public Value create(String s) {
        DoubleValue result = new DoubleValue();
        result.value = Double.parseDouble(s);
        return result;
    }

    @Override
    public Double getValue() {
        return this.value;
    }


}