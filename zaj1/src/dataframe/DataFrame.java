package dataframe;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import value.*;



public class DataFrame {

    public List<Column> columns;

    public DataFrame(String[] names, Class<? extends Value>[] types){
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



    public DataFrame(String fileName, Class<? extends Value>[] typeNames, boolean header) throws IOException {
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

                if (isUnique(colNames[i])){
                    columns.add(new Column(colNames[i], typeNames[i]));
                }
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

                if (isUnique(colNames[i])){
                    columns.add(new Column(colNames[i], typeNames[i]));
                }
            }
        }

//Read File Line By Line
        //while ((strLine = br.readLine()) != null)   {
        Class<? extends Value>[] columnClass = getClasses();
        IntegerValue integerValue = new IntegerValue(11);
        Value[] values = new Value[columns.size()];
        while ((strLine = br.readLine()) != null)   {
            String[] str = strLine.split(",");
            for (int i = 0; i < str.length; i++) {
                values[i] = integerValue.create(str[i]);
            }
            addRow(values.clone());
        }

//Close the input stream
        br.close();
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

    public int width(){
        return columns.size();
    }

    public Column get(String colname){
        for (Column col : columns){
            if (col.getName().equals(colname)) return col;
        }
        return null;
    }

    public boolean addRow(Value... objects){
        if (columns.size() != objects.length) return false;

        for (int i =0; i<columns.size(); i++) {
            String el_type = objects[i].getClass().toString();
            String col_type = columns.get(i).getType().toString();
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

    public Class<? extends Value>[] getColumnsTypes() {
        Class[] result = new Class[width()];
        for (int i = 0; i < width(); i++) {
            result[i] = columns.get(i).getType();
        }
        return result;
    }

    public String[] getColumnsNames() {
        String[] result = new String[width()];
        for (int i = 0; i < width(); i++) {
            result[i] = columns.get(i).getName();
        }
        return result;
    }

    public Class<? extends Value>[] getClasses(){
        Class[] classes = new Class[columns.size()];
        for (int i = 0; i < classes.length ; i++) {
            classes[i] = columns.get(i).getType();
        }
        return classes;
    }

}