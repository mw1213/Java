package value;

public class DoubleValue extends Value {
    double value;

    DoubleValue() {
        value = 0.0;
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
        result.value = this.value * useMe * useMe;
        return result;
    }

    @Override
    public boolean eq(Value myData) {
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        if (useMe == this.value){
            return true;
        }
        return false;
    }

    @Override
    public boolean lte(Value myData) {
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        if (useMe >= this.value){
            return true;
        }
        return false;
    }
    @Override
    public boolean gte(Value myData) {
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        if (useMe <= this.value){
            return true;
        }
        return false;
    }

    @Override
    public boolean neq(Value myData) {
        String toUse = myData.toString();
        double useMe = Double.parseDouble(toUse);
        if (useMe != this.value){
            return true;
        }
        return false;
    }
    @Override
    public boolean equals(Object other) {
        if (this.hashCode() == other.hashCode()) return true;
        return false;
    }

    @Override
    public int hashCode() {
        int code = Double.hashCode(this.value);
        return code;
    }

    @Override
    public DoubleValue create(String s) {
        DoubleValue result = new DoubleValue();
        result.value = Double.parseDouble(s);
        return result;
    }


}
