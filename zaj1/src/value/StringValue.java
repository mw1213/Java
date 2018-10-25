package value;

import java.lang.Math;

public class StringValue extends Value {
    private String value;

    StringValue(){
        value = null;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public Value add(Value myData) {
        StringValue result = new StringValue();
        String toUse = myData.toString();
        result.value = this.value + toUse;
        return result;
    }

    @Override
    public Value sub(Value myData) {
        StringValue result = new StringValue();
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        double doubleMe = Double.parseDouble(this.value);
        double resultDouble = doubleMe - useMe;
        result.value = Double.toString(resultDouble);
        return result;
    }


    @Override
    public Value mul(Value myData) {
        StringValue result = new StringValue();
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        double doubleMe = Double.parseDouble(this.value);
        double resultDouble = doubleMe * useMe;
        result.value = Double.toString(resultDouble);
        return result;
    }


    @Override
    public Value div(Value myData) {
        StringValue result = new StringValue();
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        double doubleMe = Double.parseDouble(this.value);
        double resultDouble = doubleMe / useMe;
        result.value = Double.toString(resultDouble);
        return result;
    }

    @Override
    public Value pow(Value myData) {
        StringValue result = new StringValue();
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        double doubleMe = Double.parseDouble(this.value);
        double resultDouble = Math.pow(doubleMe, useMe);
        result.value = Double.toString(resultDouble);
        return result;
    }

    @Override
    public boolean eq(Value myData) {
        String toUse = myData.toString();
        return toUse.equals(this.value);
    }

    @Override
    public boolean lte(Value myData) {
        return false;
    }

    @Override
    public boolean gte(Value myData) {
        return false;
    }

    @Override
    public boolean neq(Value myData) {
        String toUse = myData.toString();
        return !toUse.equals(this.value);
    }

    @Override
    public boolean equals(Object other) {
        return this.hashCode() == other.hashCode();
    }

    @Override
    public int hashCode() {
        int code = this.value.hashCode();
        return code;
    }

    @Override
    public Value create(String s) {
        StringValue result = new StringValue();
        result.value = s;
        return result;
    }

    @Override
    public String getValue(){
        return this.value;
    }
}
