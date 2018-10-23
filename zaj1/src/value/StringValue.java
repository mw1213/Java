package value;

public class StringValue extends Value {
    String value;

    StringValue(){
        value = null;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public Value add(Value a) {
        return null;
    }

    @Override
    public Value sub(Value a) {
        return null;
    }

    @Override
    public Value mul(Value a) {
        return null;
    }

    @Override
    public Value div(Value a) {
        return null;
    }

    @Override
    public Value pow(Value a) {
        return null;
    }

    @Override
    public boolean eq(Value a) {
        return false;
    }

    @Override
    public boolean lte(Value a) {
        return false;
    }

    @Override
    public boolean gte(Value a) {
        return false;
    }

    @Override
    public boolean neq(Value a) {
        return false;
    }

    @Override
    public boolean equals(Object other) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public Value create(String s) {
        return null;
    }
}
