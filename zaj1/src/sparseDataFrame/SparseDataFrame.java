package SparseDataFrame;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import dataframe.*;
import value.*;

public class SparseDataFrame extends DataFrame {

    public List<SparseColumn> columns;

    public SparseDataFrame(){
        columns = new ArrayList<>();
    }

    public SparseDataFrame(String[] colNames, Class<? extends Value>[] typeNames, Value _hide){
        columns = new ArrayList<>(colNames.length);
        for (String name : colNames){
            columns.add(new SparseColumn(name, typeNames[0], _hide));
        }
    }

    public SparseDataFrame(DataFrame dataFrame, Value _hide){
        if (dataFrame.width() > 0) {
            Class<? extends Value>[] columnTypes = dataFrame.getColumnsTypes();
            Class<? extends Value> firstType = columnTypes[0];
            for (Class<? extends Value> type : columnTypes){
                if (!type.equals(firstType))
                    throw new UnsupportedOperationException("Has more than one type of data in it.");
            }
            columns = new ArrayList<>();
            String [] columnNames = dataFrame.getColumnsNames();
            for (String name : columnNames){
                columns.add(new SparseColumn(name, firstType, _hide));
            }

            for (int i=0; i<dataFrame.size();i++){
                Value[] addingRow = new Value[dataFrame.width()];
                for (int j=0; j<dataFrame.width(); j++){
                    addingRow[j] = dataFrame.columns.get(j).elAtIndex(i);
                }
                addRow(addingRow);
            }

        }
    }
/*
    public SparseDataFrame(String fileName, String [] typeNames, boolean header, String _hide) throws IOException {
        columns = new ArrayList<>();
        String[] colNames = new String[typeNames.length];

        if (header == false){
            System.out.println("I need colNames passed by you. Give me " + typeNames.length + " names:");
            Scanner scanner = new Scanner(System.in);
            for (int i = 0; i<typeNames.length; i++){
                colNames[i] = scanner.next();
            }

            for (int i = 0; i<typeNames.length; i++){
                if (colNames.length <= i) break;
                columns.add(new SparseColumn(colNames[i], typeNames[i], _hide));
            }
        }

        FileInputStream fstream;
        BufferedReader br;
        fstream = new FileInputStream(fileName);

        if (fstream == null)
            throw new IOException("File not found!");
        else
            br = new BufferedReader(new InputStreamReader(fstream));


        String strLine;

        if (header == true){
            strLine = br.readLine();
            colNames = strLine.split(",");
            for (int i = 0; i<typeNames.length; i++){
                if (colNames.length <= i) break;

                columns.add(new SparseColumn(colNames[i], typeNames[i], _hide));
            }
        }

//Read File Line By Line
        //while ((strLine = br.readLine()) != null)   {
        for (int a=0; a<10;a++){
            strLine = br.readLine();
            String[] row = strLine.split(",");
            Value[] objects = new Value[row.length];

            if (typeNames[0].equals("Double")) {
                for (int i = 0; i < row.length; i++) {
                    objects[i] = Double.parseDouble(row[i]);
                }
                addRow(objects);
            }

            if (typeNames[0].equals("Intiger") || typeNames[0].equals("int")) {
                for (int i = 0; i < row.length; i++) {
                    objects[i] = Integer.parseInt(row[i]);
                }
                addRow(objects);
            }
        }

//Close the input stream
        br.close();
    }
*/
    @Override
    public String[] getColumnsNames(){
        String[] result = new String[width()];
        for(int i = 0; i < width(); i++){
            result[i] = columns.get(i).getName();
        }
        return result;
    }

    @Override
    public Class<? extends Value>[] getColumnsTypes(){
        Class[] result = new Class[width()];
        for(int i = 0; i < width(); i++){
            result[i] = columns.get(i).getType();
        }
        return result;
    }


    public Value getHide(){
        return columns.get(0).hide;
    }


    public DataFrame toDense(){
        String[] names = this.getColumnsNames();
        Class[] types = this.getColumnsTypes();
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
    public boolean addRow(Value... objects){
        if (columns.size() != objects.length) return false;

        for (int i =0; i<columns.size(); i++) {
            Class<? extends Value> el_type = objects[i].getClass();
            if(!el_type.equals(this.columns.getClass())) return false;
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
        Value[] addingRow = new Value[width()];
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
        Value[] addingRow = new Value[width()];
        for(int j = from; j < to; j++) {
            for (int i = 0; i < width(); i++) {
                addingRow[i] = columns.get(i).elAtIndex(j);
            }
            result.addRow(addingRow);
        }
        return result;
    }

}
