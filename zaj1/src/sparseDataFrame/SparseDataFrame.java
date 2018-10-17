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


    public DataFrame toDense(){
        DataFrame result = new DataFrame(getColumnsNames(), getColumnsTypes());
        int columnCount = getColumnsNames().length;
        for (int i = 0; i < columns.size(); i++){
            Object[] objects = new Object[columnCount];
            for (int j = 0; j < columnCount; j++){
                objects[j]=columns.get(i).elAtIndex(i);
            }
            result.addRow(objects);
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
}
