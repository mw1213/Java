package myExceptions;

import value.Value;

public class AddingWrongClassesException extends Exception{
    private Class<? extends Value> valueExpected;
    private Class<? extends Value> actualValue;

    public AddingWrongClassesException(Class<? extends Value> columnClass, Class<? extends Value> elementClass){
        this.valueExpected=columnClass;
        this.actualValue=elementClass;
    }


    public Class<? extends Value> getElementClass() {
        return valueExpected;
    }

    public Class<? extends Value> getColumnClass() {
        return actualValue;
    }

    public void printMessage(){
        System.out.println("Invalid operation for element: "+"\n" +
                "Expected type: "+valueExpected.toString()+"\nActual type: "+actualValue.toString());
    }
}
