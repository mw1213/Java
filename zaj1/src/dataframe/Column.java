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


}
