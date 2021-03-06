package value;

import java.lang.Math;

public class DoubleValue extends Value {
    private double value;
    private boolean set;
    public DoubleValue() {
        value = 0.0;
        set=true;
    }

    public DoubleValue(String s){
        try {
            value = Double.parseDouble(s);
            set = true;
        } catch (Exception e){
            set=false;
        }
    }

    public DoubleValue(double v) {
        this.value=v;
        set=true;
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
        try {
            result.value = Double.parseDouble(s);
            result.set=true;
        } catch (Exception e){
            set=false;
        }
    return result;
    }
    @Override
    public Double getValue() {
        return this.value;
    }

    @Override
    public boolean getSet() {
        return set;
    }


}