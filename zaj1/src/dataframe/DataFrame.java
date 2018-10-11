package dataframe;

import java.util.ArrayList;
import java.util.List;


public class DataFrame {

    private List<Column> columns;

    DataFrame(String[] names, String[] types){
        columns = new ArrayList<>();
        for (int i=0; i<types.length; i++){
            if (i >= names.length) break;

            if(isUnique(names[i])) {
                columns.add(new Column(names[i], types[i]));
            }
        }
    }

    public DataFrame() {
        columns = new ArrayList<>();
    }

    private boolean isUnique(String name) {
        for(Column c : columns) {
            if(c.getName().equals(name)) {
                return false;
            }
        }
        return true;
    }

    public int size(){
        if (columns.isEmpty()) return 0;
        else return columns.get(0).size();
    }

    public Column get(String colname){
        for (Column col : columns){
            if (col.getName().equals(colname)) return col;
        }
        return null;
    }

    public boolean addRow(Object... objects){
        if (columns.size() != objects.length) return false;

        for (int i =0; i<columns.size(); i++) {
            String el_type = objects[i].getClass().toString();
            String col_type = columns.get(i).getType();
            if(!el_type.contains(col_type)) return false;
        }

        for (int i =0; i<columns.size(); i++){
            columns.get(i).addElement(objects[i]);
        }
        return true;

    }


    public DataFrame get(String[] cols, boolean copy){
        DataFrame newDataFrame = new DataFrame();
        if(copy){
            for (String c : cols){
                for (Column col : columns){
                    if (col.getName().equals(c)){
                        Column addColumn = new Column(col);
                        newDataFrame.columns.add(addColumn);
                        break;
                    }
                }
            }
            return newDataFrame;
        }
        else {
            for (String c : cols){
                for (Column col : columns){
                    if (col.getName().equals(c)){
                        newDataFrame.columns.add(col);
                    }
                }
            }
            return newDataFrame;
        }

    }

    public DataFrame iloc (int i){
        DataFrame output = new DataFrame();

        for(Column c : columns) {
            Column column = new Column(c.getName(), c.getType());
            if(this.size() > i && i >= 0) {
                column.addElement(c.elAtIndex(i));
            }
            output.columns.add(column);
        }

        return output;
    }

    public DataFrame iloc (int from, int to){
        DataFrame output = new DataFrame();
        from = (from < 0) ? 0 : from;

        for (Column c: columns) {
            Column column = new Column(c.getName(), c.getType());
            for(int i = from; (i <= to) && (i < size()); i++) {
                column.addElement(c.elAtIndex(i));
            }
            output.columns.add(column);
        }

        return output;
    }

    @Override
    public String toString() {
        StringBuilder out = new StringBuilder();
        for(Column c : columns) {
            out.append(c.toString());
        }
        return out.toString();
    }

}