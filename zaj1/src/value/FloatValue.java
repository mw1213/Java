package value;

import myExceptions.AddingWrongClassesException;

import java.lang.Math;

public class FloatValue extends Value {
    private float value;
    private boolean set;
    public FloatValue() {
        value = (float) 0.0;
        set=true;
    }

    public FloatValue(String  s) {
        try{
           value = Float.parseFloat(s);
            set = true;
        } catch (Exception e){
            set=false;
        }
    }

    public FloatValue(float  s){
        value = s;
        set=true;
    }

    @Override
    public boolean getSet(){
        return set;
    }

    @Override
    public String toString() {
        return Float.toString(value);
    }

    @Override
    public FloatValue add(Value myData) {
        FloatValue result = new FloatValue();
        String toUse = myData.toString();
        float useMe = Float.parseFloat(toUse);
        result.value = this.value + useMe;
        return result;
    }

    @Override
    public FloatValue sub(Value myData) {
        FloatValue result = new FloatValue();
        String toUse = myData.toString();
        float useMe = Float.parseFloat(toUse);
        result.value = this.value - useMe;
        return result;
    }

    @Override
    public FloatValue mul(Value myData) {
        FloatValue result = new FloatValue();
        String toUse = myData.toString();
        float useMe = Float.parseFloat(toUse);
        result.value = this.value * useMe;
        return result;
    }

    @Override
    public FloatValue div(Value myData) {
        FloatValue result = new FloatValue();
        String toUse = myData.toString();
        float useMe = Float.parseFloat(toUse);
        result.value = this.value / useMe;
        return result;
    }

    @Override
    public FloatValue pow(Value myData) {
        FloatValue result = new FloatValue();
        String toUse = myData.toString();
        float useMe = Float.parseFloat(toUse);
        result.value = (float) Math.pow(this.value, useMe);
        return result;
    }

    @Override
    public boolean eq(Value myData) {
        String toUse = myData.toString();
        float useMe = Float.parseFloat(toUse);
        if (useMe == this.value) {
            return true;
        }
        return false;
    }

    @Override
    public boolean lte(Value myData) {
        String toUse = myData.toString();
        float useMe = Float.parseFloat(toUse);
        if (useMe >= this.value) {
            return true;
        }
        return false;
    }

    @Override
    public boolean gte(Value myData){
        String toUse = myData.toString();
        float useMe = Float.parseFloat(toUse);
        if (useMe <= this.value) {
            return true;
        }
        return false;
    }

    @Override
    public boolean neq(Value myData) {
        String toUse = myData.toString();
        float useMe = Float.parseFloat(toUse);
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
        int code = Float.hashCode(this.value);
        return code;
    }

    @Override
    public Value create(String s) {
        FloatValue result = new FloatValue();
        try{
            result.value = Float.parseFloat(s);
            result.set = true;
        } catch (Exception e){
            result.set = false;
        }
        return result;
    }

    @Override
    public Float getValue() {
        return this.value;
    }
}