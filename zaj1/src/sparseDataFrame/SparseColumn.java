package SparseDataFrame;

import dataframe.*;
import java.util.ArrayList;
import java.util.List;
import value.*;

public class SparseColumn extends Column {
    protected Value hide;
    protected List<COOValue> list;
    protected int size;

    /**
     *
     * @return hide value
     */

    public Object getHide() {
        return hide;
    }

    /**
     *
     * @return list
     */
    public List<COOValue> getList() {
        return list;
    }

    /**
     *
     * @param _name gives name of the column
     * @param _type gives type of data in it
     * @param _hide gives data which shouldn't be saved
            */
    public SparseColumn(String _name, Class<? extends Value> _type, Value _hide){
        super(_name, _type);
        this.list = new ArrayList<>();
        this.hide = _hide;
    }

    /**
     * copy constructor
     * @param column gives column to copy from
     * @param _hide gives data which shouldn't be saved
     */
    public SparseColumn(Column column, Value _hide){
        super(column);
        this.hide = _hide;
    }

    /*public Object hideToType(){
        switch (type){
            case "Boolean":
                return hide;
            case "Integer":{
                int result = Integer.parseInt(hide);
                return result;
            }
            case "Float": {
                float result = Float.parseFloat(hide);
                return result;
            }
            case "Double": {
                double result = Double.parseDouble(hide);
                return result;
            }
            case "Byte": {
                byte result = Byte.parseByte(hide);
                return result;
            }
            case "Long": {
                long result = Long.parseLong(hide);
                return result;
            }
            case "Short": {
                short result = Short.parseShort(hide);
                return result;
            }
            case "Character": {
                char result = hide.charAt(0);
                return result;
            }
            default: {
                Object result = hide;
                return result;
            }
        }
    }
    */

    /**
     *
     * @param _index points on the index from which data should be return
     * @return data from index
     */

    @Override
    public Value elAtIndex (int _index){
        if (_index > size){
            throw new IllegalArgumentException("Out of size");
        }
        for(COOValue data : list){
            if (data.getIndex() == _index){
                return data.getValue();
            }
        }
        return hide;

    }

    /**
     *
     * @param el is saved to the column
     */

    @Override
    public void addElement(Value el){
        if(this.type.isInstance(el)){
            if (!el.equals(this.hide)){
                list.add(new COOValue(size, el));
                this.size++;
            }
            else {
                this.size++;
            }
        }

    }

    /**
     *
     * @return size of column
     */
    @Override
    public int size() {
        return size;
    }

    /**
     *
     * @return string for showing on the screen
     */
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append('[');
        for (int i=0; i<size;i++){
            boolean added = false;
            for (COOValue data : list){
                if(data.getIndex() == i){
                    result.append(data.getValue().toString());
                    added = true;
                }
            }
            if (!added){
                result.append(hide);
            }
            if (i < size -1){
                result.append(", ");
            }
        }
        result.append(']');
        return "Column name: " + name + " " + "Column type: " + type + " " + "Is hidden: " + hide + "\n" + result.toString();
    }


}
