package sparseDataFrame;

import myExceptions.AddingWrongClassesException;
import value.*;

public class COOValue extends Value {
    private int index;
    private Value value;

    /**
     * constructor
     * @param _index gives index to save
     * @param _value gives value to save
     */
    public COOValue(int _index, Value _value) {
        this.index = _index;
        this.value = _value;
    }


    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public Value getValue() {
        return value;
    }

    public void setValue(Value value) {
        this.value = value;
    }


    @Override
    public String toString() {
        return "COOValue{" +
                "index=" + index +
                ", value=" + value.toString()+
                '}';
    }

    @Override
    public Value add(Value a) {
        return this.value.add(a);
    }

    @Override
    public Value sub(Value a) {
        return this.value.sub(a);
    }

    @Override
    public Value mul(Value a) {
        return this.value.mul(a);
    }

    @Override
    public Value div(Value a) {
        return this.value.div(a);
    }

    @Override
    public Value pow(Value a) {
        return this.value.pow(a);
    }

    @Override
    public boolean eq(Value a) {
        return this.value.eq(a);
    }

    @Override
    public boolean lte(Value a) {
        return this.value.lte(a);
    }

    @Override
    public boolean gte(Value a) {
        return this.value.gte(a);
    }

    @Override
    public boolean neq(Value a) {
        return this.value.neq(a);
    }

    @Override
    public boolean equals(Object other) {
        return this.value.equals(other);
    }

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public Value create(String s) {
        return this.value.create(s);
    }

}
