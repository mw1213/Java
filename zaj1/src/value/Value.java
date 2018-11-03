package value;

import groupby.*;

public abstract class Value implements Cloneable {


    public abstract String toString();
    public abstract Value add(Value a);
    public abstract Value sub(Value a);
    public abstract Value mul(Value a);
    public abstract Value div(Value a);
    public abstract Value pow(Value a);
    public abstract boolean eq(Value a);
    public abstract boolean lte(Value a);
    public abstract boolean gte(Value a);
    public abstract boolean neq(Value a);

    @Override
    public abstract boolean equals(Object other);
    @Override
    public abstract int hashCode();

    public abstract Value create(String s);

    public abstract Object getValue();

    @Override
    public Value clone() throws  CloneNotSupportedException{
        Value clone = (Value)super.clone();
        return clone;
    };

}
