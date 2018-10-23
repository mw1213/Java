package dataframe;


import java.util.ArrayList;
import java.util.List;

public class Column {

    protected String name;
    protected String type;
    private List<Object> list;

    public Column(String _name, String _type){
        this.name = _name;
        this.type = fixedType(_type);
        list = new ArrayList<>();

    }

    public Column (Column col) {
        this.name = col.name;
        this.type = col.type;
        this.list = new ArrayList<>(col.list);
    }


    public void addElement(Object el){
        String el_type = el.getClass().toString();
        if(el_type.contains(this.type)) list.add(el);
    }


    public String getType() {
        return type;
    }

    public String getName(){
        return name;
    }

    public int size(){
        return list.size();
    }

    public Object elAtIndex(int index){
        return list.get(index);
    }

    private String fixedType(String type){
        switch (type){
            case "boolean":
                return "Boolean";
            case "int":
                return "Integer";
            case "float":
                return "Float";
            case "double":
                return "Double";
            case "byte":
                return "Byte";
            case "long":
                return "Long";
            case "short":
                return "Short";
            case "char":
                return "Character";
            default:
                return type;
        }

    }

    @Override
    public String toString(){
        return "Name: " + name + " type: " + type + "\n contains: " + list +"\n";
    }


}