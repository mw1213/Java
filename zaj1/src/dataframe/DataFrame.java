package dataframe;

import java.io.*;
import java.util.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import groupby.*;
import value.*;



public class DataFrame implements  Applyable {

    public List<Column> columns;

    /**]
     * normal constructor
     * @param names gives names of the columns
     * @param types gives types of data in columns
     */
    public DataFrame(String[] names, Class<? extends Value>[] types){
        columns = new ArrayList<>();
        for (int i=0; i<types.length; i++){
            if (i >= names.length) break;

            if(isUnique(names[i])) {
                columns.add(new Column(names[i], types[i]));
            }
        }
    }

    /**
     * constructor for getting empty DataFrame
     */
    public DataFrame() {
        columns = new ArrayList<>();
    }


    /**
     * constructor with reading data from file
     * @param address file from which data is read
     * @param types types of data in columns
     * @param header boolean defining weather column names are written in file or not
     * @throws IOException
     */

    public DataFrame(String address, Class<? extends Value>[] types, boolean header) throws IOException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {

        columns = new ArrayList<>();
        FileInputStream fstream;
        BufferedReader br;

        fstream = new FileInputStream(address);

        if (fstream == null)
            throw new IOException("File not found!");
        else
            br = new BufferedReader(new InputStreamReader(fstream));

        String strLine=br.readLine();
        String[] separated=strLine.split(",");
        String[] names= new String[types.length];
        if (!header){
            Scanner odczyt = new Scanner(System.in);
            for (int l=0;l<types.length;l++){
                System.out.print("Podaj nazwę kolumny: ");
                names[l] = odczyt.nextLine();
            }
        }
        if (header){
            for (int m=0;m<types.length;m++){
                names[m]=separated[m];
            }
        }
        for (int i = 0; i < types.length; i++) {
            if((separated.length <= i)) {
                break;
            }
            columns.add(new Column(names[i], types[i]));
        }
        Value[] values= new Value[columns.size()];
        List<Constructor<? extends Value>> constructors = new ArrayList<>(types.length);
        for (int i=0;i<types.length;i++){
            constructors.add(types[i].getConstructor(String.class));
        }
        //while ((strLine = br.readLine()) != null){
        for (int b=0; b<50;b++) {
            strLine = br.readLine();
            String[] str = strLine.split(",");
            for (int i = 0; i<str.length; i++){
                values[i] = constructors.get(i).newInstance(str[i]);
            }
            addRow(values);

        }
        br.close();

    }

    /**
     *
     * @param name name for new column
     * @return true when column name is unique and can be added to the dataframe
     */
    private boolean isUnique(String name) {
        for(Column c : columns) {
            if(c.getName().equals(name)) {
                return false;
            }
        }
        return true;
    }

    /**
     * by default columns are the same size
     * @return size of columns
     */
    public int size(){
        if (columns.isEmpty()) return 0;
        else return columns.get(0).size();
    }

    /**
     *
     * @return number of columns
     */
    public int width(){
        return columns.size();
    }

    /**
     *
     * @param colname column to return
     * @return one column from the dataframe
     */

    public Column get(String colname){
        for (Column col : columns){
            if (col.getName().equals(colname)) return col;
        }
        return null;
    }

    /**
     *
     * @param objects values to add to the dataframe
     * @return boolean to know if row was added
     */
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

    public boolean addRow(List<Value> values){
        IntStream.range(0, columns.size()).forEach(i -> columns.get(i).addElement(values.get(i)));
        return true;
    }

    /**
     *
     * @param cols names of columns to the new dataframe
     * @param copy boolean defining weather to create new objects or just pass another pointer to data
     * @return new dataframe from chosen columns
     */
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

    /**
     *
     * @param i index of row which should be returned
     * @return new dataframe with chosen row
     */
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

    public Value[] ilocValue (int j){
        Value[] output;
        DataFrame helpMe = this.iloc(j);
        output = new Value[helpMe.width()];
        for (int i = 0; i<helpMe.width(); i++){
            output[i] = helpMe.columns.get(i).elAtIndex(0);
        }
        return output;
    }

    /**
     *
     * @param from row from which it begins to copy
     * @param to ending row
     * @return SDF with rows from range: from - to
     */
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

    /**
     *
     * @return string for display
     */
    @Override
    public String toString() {
        StringBuilder out = new StringBuilder();
        for(Column c : columns) {
            out.append(c.toString());
        }
        return out.toString();
    }

    /**
     *
     * @return types of data in dataframe
     */
    public Class<? extends Value>[] getColumnsTypes() {
        Class[] result = new Class[width()];
        for (int i = 0; i < width(); i++) {
            result[i] = columns.get(i).getType();
        }
        return result;
    }

    /**
     *
     * @return names of columns
     */
    public String[] getColumnsNames() {
        String[] result = new String[width()];
        for (int i = 0; i < width(); i++) {
            result[i] = columns.get(i).getName();
        }
        return result;
    }



    public class DataFrameGroupBy implements Groupby{
        private HashMap<List<Value>, DataFrame> map;
        private List<String> colNames;

        public DataFrameGroupBy(HashMap<List<Value>, DataFrame> _map, String[] _colNames){
            this.map=_map;
            this.colNames= Arrays.asList(_colNames);
        }

        @Override
        public DataFrame max() {
            DataFrame result = new DataFrame(getColumnsNames(), getColumnsTypes());
            for (var values: map.keySet()){
                List<Value> toAdd = new ArrayList<>(values);
                DataFrame dataFrameHelp = map.get(values);

                for (var column: dataFrameHelp.columns){
                    if(column.list.isEmpty()) toAdd.add(null);
                    else {
                        Value max = column.list.get(0);
                        for (var value: column.list){
                            if(value.gte(max)) max=value;
                        }
                        toAdd.add(max);
                    }
                }
                result.addRow(toAdd);
            }
            return result;
        }

        @Override
        public DataFrame min() {
            DataFrame result = new DataFrame(getColumnsNames(), getColumnsTypes());
            for (var values: map.keySet()){
                List<Value> toAdd = new ArrayList<>(values);
                DataFrame dataFrameHelp = map.get(values);

                for (var column: dataFrameHelp.columns){
                    if(column.list.isEmpty()) toAdd.add(null);
                    else {
                        Value min = column.list.get(0);
                        for (var value: column.list){
                            if(value.lte(min)) min=value;
                        }
                        toAdd.add(min);
                    }
                }
                result.addRow(toAdd);
            }
            return result;
        }

        @Override
        public DataFrame mean() {
            DataFrame result = new DataFrame(getColumnsNames(), getColumnsTypes());
            for (var values : map.keySet()) {
                List<Value> toAdd = new ArrayList<>(values);
                DataFrame dataFrameHelp = map.get(values);

                for (var column : dataFrameHelp.columns) {
                    if (column.list.isEmpty()) toAdd.add(null);
                    else {
                        Value sum = column.list.get(0);
                        Value zero = column.list.get(0);
                        for (var value: column.list){
                            sum = sum.add(value);
                        }
                        sum = sum.sub(zero);
                        Value mean=sum.div(new IntegerValue(column.list.size()));
                        toAdd.add(mean);
                    }
                }
                result.addRow(toAdd);
            }
            return result;
        }


        @Override
        public DataFrame sum() {
            DataFrame result = new DataFrame(getColumnsNames(), getColumnsTypes());
            for (var values: map.keySet()){
                List<Value> toAdd = new ArrayList<>(values);
                DataFrame dataFrameHelp = map.get(values);

                for (var column: dataFrameHelp.columns){
                    if(column.list.isEmpty()) toAdd.add(null);
                    else {
                        Value sum = column.list.get(0);
                        Value zero = column.list.get(0);
                        for (var value: column.list){
                            sum = sum.add(value);
                        }
                        toAdd.add(sum.sub(zero));
                    }
                }
                result.addRow(toAdd);
            }
            return result;
        }


        @Override
        public DataFrame std() {
            return null;
        }

        @Override
        public DataFrame var() {
            return null;
        }

        @Override
        public DataFrame apply(Applyable applyable) {
            return null;
        }
    }

    public Column getColumn(String name){
        return columns.stream().filter(c -> c.getName().equals(name)).findFirst().orElse(null);
    }

    public Value[] getRow(int index){
        return columns.stream().map(column -> column.elAtIndex(index)).toArray(Value[]::new);
    }
    /**
     * implementin Groupby interface methods
     * @param groupBy name of column to sort for
     * @return inner class having smaller data frames which are sorted by the column given in @param s
     */
    public DataFrameGroupBy grupby(String[] groupBy) {
        HashMap<List<Value>, DataFrame> forResult = new HashMap<>(groupBy.length);
        List<Column> columns = Arrays.stream(groupBy).map(this::getColumn).collect(Collectors.toList());
        for (int i =0; i< size(); i++){
            List<Value> values = new ArrayList<>(columns.size());
            for (var column: columns){
                values.add(column.elAtIndex(i));
            }
            if (!forResult.containsKey(values)){
                forResult.put(values, iloc(i));
            }
            else {
                forResult.get(values).addRow(getRow(i));
            }
        }
        return new DataFrameGroupBy(forResult, groupBy);
    }

    @Override
    public DataFrame apply(DataFrame data) {
        return null;
    }
}