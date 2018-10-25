package value;


public class IntegerValue extends Value {
    private int value;


    public IntegerValue(int value) {
        this.value = value;
    }

    public IntegerValue() {
        this.value = 0;
    }

    @Override
    public String toString(){
        return Integer.toString(value);
    }

    @Override
    public IntegerValue add(Value myData){
        IntegerValue result = new IntegerValue();
        String toUse = myData.toString();
        int useMe = Integer.parseInt(toUse);
        result.value = this.value + useMe;
        return result;
    }

    @Override
    public IntegerValue sub(Value myData){
        IntegerValue result = new IntegerValue();
        String toUse = myData.toString();
        int useMe = Integer.parseInt(toUse);
        result.value = this.value - useMe;
        return result;
    }

    @Override
    public IntegerValue mul(Value myData){
        IntegerValue result = new IntegerValue();
        String toUse = myData.toString();
        int useMe = Integer.parseInt(toUse);
        result.value = this.value * useMe;
        return result;
    }

    @Override
    public IntegerValue div(Value myData){
        IntegerValue result = new IntegerValue();
        String toUse = myData.toString();
        int useMe = Integer.parseInt(toUse);
        result.value = this.value / useMe;
        return result;
    }

    @Override
    public IntegerValue pow(Value myData){
        IntegerValue result = new IntegerValue();
        String toUse = myData.toString();
        int useMe = Integer.parseInt(toUse);
        result.value = this.value ^ useMe;
        return result;
    }

    @Override
    public boolean eq(Value myData){
        String toUse = myData.toString();
        int useMe = Integer.parseInt(toUse);
        if (useMe == this.value){
            return true;
        }
        return false;
    }

    @Override
    public boolean lte(Value myData){
        String toUse = myData.toString();
        int useMe = Integer.parseInt(toUse);
        if (useMe >= this.value){
            return true;
        }
        return false;
    }

    @Override
    public boolean gte(Value myData){
        String toUse = myData.toString();
        int useMe = Integer.parseInt(toUse);
        if (useMe <= this.value){
            return true;
        }
        return false;
    }

    @Override
    public boolean neq(Value myData){
        String toUse = myData.toString();
        int useMe = Integer.parseInt(toUse);
        if (useMe != this.value){
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object other) {
        String toUse = other.toString();
        int useMe = Integer.parseInt(toUse);
        if (useMe == this.value) return true;
        return false;
    }

    @Override
    public int hashCode(){
        int result = this.value;
        return result;
    }

    @Override
    public Value create(String s){
        IntegerValue result = new IntegerValue();
        result.value = Integer.parseInt(s);
        return result;
    }

    @Override
    public Integer getValue(){
        return this.value;
    }

}