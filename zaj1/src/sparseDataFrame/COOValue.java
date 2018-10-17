package sparseDataFrame;

public class COOValue {
    private int index;
    private Object value;

    public COOValue(int _index, Object _value) {
        this.index = _index;
        this.value = _value;
    }


    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }


    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }


    @Override
    public String toString() {
        return "COOValue{" +
                "index=" + index +
                ", value=" + value +
                '}';
    }
}
