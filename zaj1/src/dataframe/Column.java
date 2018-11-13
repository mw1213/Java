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


    /**
     *
     * @return type of column
     */
    public Class<? extends Value> getType() {
        return type;
    }

    /**
     *
     * @return name of column
     */
    public String getName(){
        return name;
    }

    /**
     *
     * @return number of elements in this.list
     */
    public int size(){
        return list.size();
    }

    /**
     *
     * @param index
     * @return value on index given in @param index
     */
    public Value elAtIndex(int index){
        return list.get(index);
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

    public Column add(Value value){
        Column result = new Column(this.name, this.type);
        for (Value val: list){
            result.addElement(val.add(value));
        }
        return result;
    }

    public Column sub(Value value){
        Column result = new Column(this.name, this.type);
        for (Value val: list){
            result.addElement(val.sub(value));
        }
        return result;
    }

    public Column mul(Value value){
        Column result = new Column(this.name, this.type);
        for (Value val: list){
            result.addElement(val.mul(value));
        }
        return result;
    }

    public Column div(Value value){
        Column result = new Column(this.name, this.type);
        for (Value val: list){
            result.addElement(val.div(value));
        }
        return result;
    }

    public Column add(Column column){
        Column result = new Column(this.name, this.type);
        for (int i =0; i<list.size(); i++){
            result.addElement(list.get(i).add(column.elAtIndex(i)));
        }
        return result;
    }

    public Column sub(Column column){
        Column result = new Column(this.name, this.type);
        for (int i =0; i<list.size(); i++){
            result.addElement(list.get(i).sub(column.elAtIndex(i)));
        }
        return result;
    }

    public Column mul(Column column){
        Column result = new Column(this.name, this.type);
        for (int i =0; i<list.size(); i++){
            result.addElement(list.get(i).mul(column.elAtIndex(i)));
        }
        return result;
    }

    public Column div(Column column){
        Column result = new Column(this.name, this.type);
        for (int i =0; i<list.size(); i++){
            result.addElement(list.get(i).div(column.elAtIndex(i)));
        }
        return result;
    }

}
