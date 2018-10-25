package SparseDataFrame;

import dataframe.*;
import java.util.ArrayList;
import java.util.List;
import value.*;

public class SparseColumn extends Column {
    protected Value hide;
    protected List<COOValue> list;
    protected int size;

    public Object getHide() {
        return hide;
    }

    public List<COOValue> getList() {
        return list;
    }

    public SparseColumn(String _name, Class<? extends Value> _type, Value _hide){
        super(_name, _type);
        this.list = new ArrayList<>();
        this.hide = _hide;
    }

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

    @Override
    public int size() {
        return size;
    }


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
