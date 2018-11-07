package dataframe;


import java.util.ArrayList;
import java.util.List;
import value.*;

public class Column {

    protected String name;
    protected Class<? extends Value> type;
    protected List<Value> list;

    public Column(String _name, Class<? extends Value> _type){
        this.name = _name;
        this.type = _type;
        list = new ArrayList<>();

    }

    public Column (Column col) {
        this.name = col.name;
        this.type = col.type;
        this.list = new ArrayList<>(col.list);
    }


    public void addElement(Value el){
        if (type.isInstance(el)) list.add(el);
    }


    public Class<? extends Value> getType() {
        return type;
    }

    public String getName(){
        return name;
    }

    public int size(){
        return list.size();
    }

    public Value elAtIndex(int index){
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

    /**
     * checking if element of list is having same type as column
     * @param element given to compare types
     * @return
     */
    public boolean checkElement (Value element){
        if (type.isInstance(element)) return true;
        return false;
    }

    public void changeMyList(Value el){
        for (int i = 0 ; i<list.size(); i++){
            list.get(i).add(el);
        }
    }



}
