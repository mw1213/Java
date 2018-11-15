package dataframe;

import java.util.ArrayList;
import java.util.List;
import myExceptions.*;

import myExceptions.DifferentSizesOfColumnException;
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


    public void addElement(Value el) throws AddingWrongClassesException{
        if (type.isInstance(el)) {
            list.add(el);
        }
        else{
            throw new AddingWrongClassesException(this.getType(), el.getClass());
        }
    }

    public  void addElementRetarded(Value el){
        list.add(el);
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

    public Column add(Value value) throws AddingWrongClassesException {
        Column result = new Column(this.name, this.type);
        for (Value val: list){
            result.addElement(val.add(value));
        }
        return result;
    }

    public Column sub(Value value) throws AddingWrongClassesException {
        Column result = new Column(this.name, this.type);
        for (Value val: list){
            result.addElement(val.sub(value));
        }
        return result;
    }

    public Column mul(Value value) throws AddingWrongClassesException {
        Column result = new Column(this.name, this.type);
        for (Value val: list){
            result.addElement(val.mul(value));
        }
        return result;
    }

    public Column div(Value value) throws AddingWrongClassesException {
        Column result = new Column(this.name, this.type);
        for (Value val: list){
            result.addElement(val.div(value));
        }
        return result;
    }

    public Column add(Column column) throws DifferentSizesOfColumnException, AddingWrongClassesException {
        if(this.size()!=column.size()){
            throw new DifferentSizesOfColumnException(this.getName(), column.getName(), this.size(), column.size());
        }
        Column result = new Column(this.name, this.type);
        for (int i =0; i<list.size(); i++){
            result.addElement(list.get(i).add(column.elAtIndex(i)));
        }
        return result;
    }

    public Column sub(Column column) throws DifferentSizesOfColumnException, AddingWrongClassesException {
        if(this.size()!=column.size()){
            throw new DifferentSizesOfColumnException(this.getName(), column.getName(), this.size(), column.size());
        }
        Column result = new Column(this.name, this.type);
        for (int i =0; i<list.size(); i++){
            result.addElement(list.get(i).sub(column.elAtIndex(i)));
        }
        return result;
    }

    public Column mul(Column column) throws DifferentSizesOfColumnException, AddingWrongClassesException {
        if(this.size()!=column.size()){
            throw new DifferentSizesOfColumnException(this.getName(), column.getName(), this.size(), column.size());
        }
        Column result = new Column(this.name, this.type);
        for (int i =0; i<list.size(); i++){
            result.addElement(list.get(i).mul(column.elAtIndex(i)));
        }
        return result;
    }

    public Column div(Column column) throws DifferentSizesOfColumnException, AddingWrongClassesException {
        if(this.size()!=column.size()){
            throw new DifferentSizesOfColumnException(this.getName(), column.getName(), this.size(), column.size());
        }
        Column result = new Column(this.name, this.type);
        for (int i =0; i<list.size(); i++){
            result.addElement(list.get(i).div(column.elAtIndex(i)));
        }
        return result;
    }

    public void changeElementToWrong(Value val, int index){
        list.set(index, val);
    }
}
