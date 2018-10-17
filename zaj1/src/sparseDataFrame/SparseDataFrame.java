package sparseDataFrame;

import java.util.*;
import dataframe.*;


public class SparseDataFrame extends DataFrame {

    public List<SparseColumn> columns;

    public SparseDataFrame(){
        columns = new ArrayList<>();
    }

    public SparseDataFrame(String[] colNames, String[] typeNames, String _hide){
        columns = new ArrayList<>(colNames.length);
        for (String name : colNames){
            columns.add(new SparseColumn(name, typeNames[0], _hide));
        }
    }

    public SparseDataFrame(DataFrame dataFrame, String _hide){
        if (dataFrame.width() > 0) {
            String[] columnTypes = dataFrame.getColumnsTypes();
            String firstType = columnTypes[0];
            for (String type : columnTypes){
                if (!type.equals(firstType))
                    throw new UnsupportedOperationException("Has more than one type of data in it.");
            }
            columns = new ArrayList<>();
            String [] columnNames = dataFrame.getColumnsNames();
            for (String name : columnNames){
                columns.add(new SparseColumn(name, firstType, _hide));
            }

            for (int i=0; i<dataFrame.size();i++){
                Object[] addingRow = new Object[dataFrame.width()];
                for (int j=0; j<dataFrame.width(); j++){
                    addingRow[j] = dataFrame.columns.get(j).elAtIndex(i);
                }
                addRow(addingRow);
            }

        }
    }

    public String[] getColumnsNames(){
        String[] result = new String[width()];
        for(int i = 0; i < width(); i++){
            result[i] = columns.get(i).getName();
        }
        return result;
    }
    public String[] getColumnsTypes(){
        String[] result = new String[width()];
        for(int i = 0; i < width(); i++){
            result[i] = columns.get(i).getType();
        }
        return result;
    }

    public String getHide(){
        return columns.get(0).hide;
    }


    public DataFrame toDense(){
        String[] names = this.getColumnsNames();
        String[] types = this.getColumnsTypes();
        DataFrame result = new DataFrame();


        for (SparseColumn col : columns){
            Column column = new Column(col.getName(), col.getType());
            for (int i = 0; i < col.size(); i++){
                column.addElement(col.elAtIndex(i));
            }
            result.columns.add(column);
        }

        return result;
    }

    @Override
    public boolean addRow(Object... objects){
        if (columns.size() != objects.length) return false;

        for (int i =0; i<columns.size(); i++) {
            String el_type = objects[i].getClass().toString();
            String col_type = columns.get(i).getType();
            if(!el_type.contains(col_type)) return false;
        }

        for(int i = 0; i < objects.length; i++){
            columns.get(i).addElement(objects[i]);
        }
        return true;
    }

    public String printSize(){
        return "Size: " + columns.get(0).size;
    }

    public void printCOOValue(){
        for(SparseColumn col : columns){
            System.out.println(col.list);
        }
    }

    @Override
    public int width(){
        return columns.size();
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for(Column column : columns){
            sb.append(column.toString());
            sb.append('\n');
        }
        return sb.toString();
    }

    @Override
    public SparseColumn get(String colName){
        for (SparseColumn col : columns){
            if(col.getName().equals(colName)){
                SparseColumn column = new SparseColumn(col.getName(), col.getType(), col.hide);
                for (int i = 0; i < col.size(); i++){
                    column.addElement(col.elAtIndex(i));
                }
                return column;
            }
        }
        return null;
    }

    @Override
    public SparseDataFrame get(String[] colNames, boolean copy){
        SparseDataFrame result = new SparseDataFrame();
        if(copy){
            for (String c : colNames){
                for (SparseColumn col : columns){
                    if (col.getName().equals(c)){
                        SparseColumn addColumn = new SparseColumn(col.getName(), col.getType(), col
                        .hide);
                        for (int i = 0; i < col.size(); i++){
                            addColumn.addElement(col.elAtIndex(i));
                        }
                        result.columns.add(addColumn);
                        break;
                    }
                }
            }
            return result;
        }
        else {
            for (String c : colNames){
                for (SparseColumn col : columns){
                    if (col.getName().equals(c)){
                        result.columns.add(col);
                    }
                }
            }
            return result;
        }
    }

    @Override
    public SparseDataFrame iloc (int i){
        SparseDataFrame result = new SparseDataFrame(getColumnsNames(), getColumnsTypes(), getHide());
        Object[] addingRow = new Object[width()];
        for (int j = 0; j<width(); j++){
            addingRow[j] = columns.get(j).elAtIndex(i);
        }
        result.addRow(addingRow);
        return result;
    }

    @Override
    public SparseDataFrame iloc (int from, int to){
        SparseDataFrame result = new SparseDataFrame(getColumnsNames(), getColumnsTypes(), getHide());
        from = (from < 0) ? 0 : from;
        to = (to > columns.get(0).size) ?  columns.get(0).size : to;
        Object[] addingRow = new Object[width()];
        for(int j = from; j < to; j++) {
            for (int i = 0; i < width(); i++) {
                addingRow[i] = columns.get(i).elAtIndex(j);
            }
            result.addRow(addingRow);
        }
        return result;
    }

}
