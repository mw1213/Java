package myExceptions;

import value.StringValue;
import value.Value;

public class WrongTypeInColumnException extends Exception {
    private int index;
    private Class<? extends Value> valueExpected;
    private Class<? extends Value> actualValue;
    private String colName;

    public WrongTypeInColumnException(int _index, Class<? extends Value> columnClass, Class<? extends Value> elementClass, String colName){
        this.index=_index;
        this.valueExpected=columnClass;
        this.actualValue=elementClass;
        this.colName=colName;
    }

    public int getIndex() {
        return index;
    }

    public Class<? extends Value> getValueExpected() {
        return valueExpected;
    }

    public Class<? extends Value> getActualValue() {
        return actualValue;
    }
    public String getColName(){
        return colName;
    }

    public void printMessage(){
        System.out.println("Invalid operation for column: "+colName+ "\ntypes incompatible in index: "+index+"\n" +
                "Expected type: "+valueExpected.toString()+"\nActual type: "+actualValue.toString());
    }
}
