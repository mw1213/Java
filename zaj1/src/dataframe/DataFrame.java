package dataframe;

import java.io.*;
import java.util.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.stream.Collectors;

import groupby.*;
import myExceptions.AddingWrongClassesException;
import myExceptions.WrongTypeInColumnException;
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

    public DataFrame(String address, Class<? extends Value>[] types, boolean header) throws IOException, WrongTypeInColumnException {

        columns = new ArrayList<>();
        FileInputStream fstream;
        BufferedReader br;

        try{
            fstream = new FileInputStream(address);


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
            while ((strLine = br.readLine()) != null){
                //for (int b=0; b<50;b++) {
                //strLine = br.readLine();
                String[] str = strLine.split(",");
                for (int i = 0; i<str.length; i++){
                    try {
                        values[i] = constructors.get(i).newInstance(str[i]);
                    } catch (InvocationTargetException e) {
                        throw new WrongTypeInColumnException(0, columns.get(i).getType(), new StringValue().getClass(), columns.get(i).getName());
                    }
                }
                addRow(values);

            }
            br.close();

        }
        catch (FileNotFoundException s){
            throw new IOException("Flie not found!");
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            throw new IOException("Method in reading file wasn't file");
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new IOException("newInstance was't able to perform");
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new IOException("Illegal access to variables");
        }

    }


    /**
     * constructor for reading from file with max elements to read
     * @param address file from which data is read
     * @param types types of data in columns
     * @param header boolean defining weather column names are written in file or not
     * @param b max elements to read
     * @throws IOException
     * @throws WrongTypeInColumnException
     */
    public DataFrame(String address, Class<? extends Value>[] types, boolean header, int b) throws IOException, WrongTypeInColumnException {

        columns = new ArrayList<>();
        FileInputStream fstream;
        BufferedReader br;

        try{
            fstream = new FileInputStream(address);


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
              for (int a=0; a<b;a++) {
                strLine = br.readLine();
                String[] str = strLine.split(",");
                for (int i = 0; i<str.length; i++){
                    try {
                        values[i] = constructors.get(i).newInstance(str[i]);
                    } catch (InvocationTargetException e) {
                        throw new WrongTypeInColumnException(a, columns.get(i).getType(), new StringValue().getClass(), columns.get(i).getName());
                    }
                }
                addRow(values);

            }
            br.close();

        }
        catch (FileNotFoundException s){
            throw new IOException("Flie not found!");
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            throw new IOException("Method in reading file wasn't file");
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new IOException("newInstance was't able to perform");
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new IOException("Illegal access to variables");
        }

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

    public Column get (int index){
        return columns.get(index);
    }

    /**
     *
     * @param objects values to add to the dataframe
     * @return boolean to know if row was added
     */
    public boolean addRow(Value... objects) throws WrongTypeInColumnException{
        if (columns.size() != objects.length) return false;

        for (int i =0; i<columns.size(); i++) {
            String el_type = objects[i].getClass().toString();
            String col_type = columns.get(i).getType().toString();
            if(!el_type.contains(col_type)) return false;
        }

        for (int i =0; i<columns.size(); i++){
            try {
                columns.get(i).addElement(objects[i]);
            } catch (AddingWrongClassesException e) {
                throw new WrongTypeInColumnException(i, e.getColumnClass(), e.getElementClass(), columns.get(i).getName());
            }
        }
        return true;

    }

    /**
     * List<value> values can be bigger than columns.size() so I have to delete them from the list
     * @param values
     * @return true if adding is successful
     */
    public boolean addRow(List<Value> values) throws IOException, WrongTypeInColumnException {
        try {
            List<Value> addingValues = new ArrayList<>(values);
            List<Integer> indexesToRemove = new ArrayList<>();
            List<Class<? extends Value>> classList = new ArrayList<>(List.of(getColumnsTypes()));
            for (int i = 0; i < values.size(); i++) {
                Class<? extends Value> myClass = values.get(i).getClass();
                if (!classList.contains(myClass)) indexesToRemove.add(i);
            }
            for (int i = indexesToRemove.size() - 1; i >= 0; i--) {
                addingValues.remove((int) indexesToRemove.get(i));
            }

            for (int i = 0; i < columns.size(); i++) {
                try {
                    columns.get(i).addElement(addingValues.get(i));
                } catch (AddingWrongClassesException e) {
                    throw new WrongTypeInColumnException(i, e.getColumnClass(),e.getElementClass(), columns.get(i).getName());
                }
            }
            return true;
        }
        catch (IndexOutOfBoundsException e){
            throw new IOException("Adding to many elements to the row!");
        }
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
    public DataFrame iloc (int i)  throws WrongTypeInColumnException{
        DataFrame output = new DataFrame();

        for(Column c : columns) {
            Column column = new Column(c.getName(), c.getType());
            if(this.size() > i && i >= 0) {
                try {
                    column.addElement(c.elAtIndex(i));
                } catch (AddingWrongClassesException e) {
                    throw new WrongTypeInColumnException(i, c.getType(), c.elAtIndex(i).getClass(), c.getName());
                }
            }
            output.columns.add(column);
        }

        return output;
    }
    public DataFrame ilocGrupBy (int i) {
        DataFrame output = new DataFrame();

        for(Column c : columns) {
            Column column = new Column(c.getName(), c.getType());
            if(this.size() > i && i >= 0) {
                column.addElementRetarded(c.elAtIndex(i));
            }
            output.columns.add(column);
        }

        return output;
    }


    /**
     *
     * @param from row from which it begins to copy
     * @param to ending row
     * @return SDF with rows from range: from - to
     */
    public DataFrame iloc (int from, int to) throws WrongTypeInColumnException {
        DataFrame output = new DataFrame();
        from = (from < 0) ? 0 : from;

        for (Column c: columns) {
            Column column = new Column(c.getName(), c.getType());
            for(int i = from; (i <= to) && (i < size()); i++) {
                try {
                    column.addElement(c.elAtIndex(i));
                } catch (AddingWrongClassesException e) {
                    System.out.println("REEEEEEEEEEEEEEEEEE");
                    e.printMessage();
                    throw new WrongTypeInColumnException(i, c.getType(), c.elAtIndex(i).getClass(), c.getName());
                }
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


    /**
     * inner class making hash map of Dataframes where keys are values for which original DataFrame is sorted eg. "id"
     * and values are smaller dataframes cut from original
     */
    public class DataFrameGroupBy implements Groupby{

        private HashMap<List<Value>, DataFrame> map;
        private List<String> colNames;

        /**
         * constructor for inner class
         * @param _map gives map to store in this.map
         * @param _colNames gives colNames to store in this.colNames
         */
        public DataFrameGroupBy(HashMap<List<Value>, DataFrame> _map, String[] _colNames){
            this.map=_map;
            this.colNames= Arrays.asList(_colNames);
        }

        /**
         * max is calculated by comparing each element of each column with max for that column
         *  @return new dataframe which has only one column with max values for each value which original dataframe is sorted by eg. "id"
         */
        @Override
        public DataFrame max() throws WrongTypeInColumnException {
            DataFrame result = new DataFrame(getColumnsNames(), getColumnsTypes());
            for (var values: map.keySet()){
                List<Value> toAdd = new ArrayList<>(values);
                DataFrame dataFrameHelp = map.get(values);

                for (var column: dataFrameHelp.columns){
                    if(colNames.contains(column.getName())){
                        continue;
                    }

                    Value max = column.list.get(0);
                    for (int i=0; i<column.list.size();i++){
                        var value = column.elAtIndex(i);

                        if(value.getClass().equals(column.getType())){
                            if(value.gte(max)) max=value;
                        }
                        else {
                            throw new WrongTypeInColumnException(i, column.getType(), value.getClass(), column.getName());
                        }


                    }
                    toAdd.add(max);

                }
                try{
                    result.addRow(toAdd);
                }
                catch (IOException e){
                    System.out.println("Adding elements to DataFrame unsuccessful");
                }
            }
            return result;
        }

        /**
         * min is calculated by comparing each element of each column with min for that column
         * @return new dataframe which has only one column with min values for each value which original dataframe is sorted by eg. "id"
         */
        @Override
        public DataFrame min() throws WrongTypeInColumnException {
            DataFrame result = new DataFrame(getColumnsNames(), getColumnsTypes());
            for (var values: map.keySet()){
                List<Value> toAdd = new ArrayList<>(values);
                DataFrame dataFrameHelp = map.get(values);

                for (var column: dataFrameHelp.columns){
                    if(colNames.contains(column.getName())){
                        continue;
                    }
                    Value min = column.list.get(0);
                    for (var value: column.list){
                        if(value.lte(min)) min=value;
                    }
                    toAdd.add(min);
                }
                try{
                    result.addRow(toAdd);
                }
                catch (IOException e){
                    System.out.println(e);
                    System.out.println("Adding elements to DataFrame unsuccessful");
                }
            }
            return result;
        }

        /**
         * mean is calculated by dividing sum by number of elements
         * @return new dataframe which has only one column with mean values for each value which original dataframe is sorted by eg. "id"
         */
        @Override
        public DataFrame mean() throws WrongTypeInColumnException {
            List<Class<? extends Value>> classList = new ArrayList<>(List.of(getColumnsTypes()));
            ArrayList<String> nameList = new ArrayList<>(List.of(getColumnsNames()));
            List<Integer> indexesToRemove = new ArrayList<>();
            for (int i = 0; i<classList.size(); i++){
                if ((classList.get(i).equals(StringValue.class) || classList.get(i).equals(DateTimeValue.class)) && !colNames.contains(nameList.get(i))){
                    indexesToRemove.add(i);
                }
            }
            for (int i = indexesToRemove.size()-1; i>=0; i--){
                nameList.remove((int)indexesToRemove.get(i));
                classList.remove((int)indexesToRemove.get(i));
            }
            String[] columnNames = nameList.toArray(String[]::new);
            Class[] types = classList.toArray(Class[]::new);
            DataFrame result = new DataFrame(columnNames, types);
            for (var values : map.keySet()) {
                List<Value> toAdd = new ArrayList<>(values);
                DataFrame dataFrameHelp = map.get(values);

                for (var column : dataFrameHelp.columns) {
                    if(colNames.contains(column.getName())){
                        continue;
                    }
                    Value sum = column.list.get(0);
                    Value zero = column.list.get(0);
                    for (var value : column.list) {
                        sum = sum.add(value);
                    }
                    sum = sum.sub(zero);
                    Value mean = sum.div(new IntegerValue(column.list.size()));
                    toAdd.add(mean);
                }
                try{
                    result.addRow(toAdd);
                }
                catch (IOException e){
                    System.out.println("Adding elements to DataFrame unsuccessful");
                }
            }
            return result;
        }

        /**
         * sums every element in each column
         * @return new dataframe which has only one column with sum values for each value which original dataframe is sorted by eg. "id"
         */
        @Override
        public DataFrame sum() throws WrongTypeInColumnException {
            List<Class<? extends Value>> classList = new ArrayList<>(List.of(getColumnsTypes()));
            ArrayList<String> nameList = new ArrayList<>(List.of(getColumnsNames()));
            List<Integer> indexesToRemove = new ArrayList<>();
            for (int i = 0; i<classList.size(); i++){
                if ((classList.get(i).equals(StringValue.class) || classList.get(i).equals(DateTimeValue.class)) && !colNames.contains(nameList.get(i))){
                    indexesToRemove.add(i);
                }
            }
            for (int i = indexesToRemove.size()-1; i>=0; i--){
                nameList.remove((int)indexesToRemove.get(i));
                classList.remove((int)indexesToRemove.get(i));
            }
            String[] columnNames = nameList.toArray(String[]::new);
            Class[] types = classList.toArray(Class[]::new);
            DataFrame result = new DataFrame(columnNames, types);
            for (var values: map.keySet()){
                List<Value> toAdd = new ArrayList<>(values);
                DataFrame dataFrameHelp = map.get(values);

                for (var column: dataFrameHelp.columns){
                    if(colNames.contains(column.getName())){
                        continue;
                    }
                    if(!column.getClass().equals(DateTimeValue.class)) {
                        Value sum = column.list.get(0);
                        Value zero = column.list.get(0);
                        for (var value : column.list) {
                            sum = sum.add(value);
                        }
                        toAdd.add(sum.sub(zero));
                    }
                }
                try{
                    result.addRow(toAdd);
                }
                catch (IOException e){
                    System.out.println("Adding elements to DataFrame unsuccessful");
                }
            }
            return result;
        }

        /**
         * uses var method and then Math.sqrt(var)
         * @return new dataframe which has only one column with std values for each value which original dataframe is sorted by eg. "id"
         */
        @Override
        public DataFrame std() throws WrongTypeInColumnException {
            List<Class<? extends Value>> classList = new ArrayList<>(List.of(getColumnsTypes()));
            ArrayList<String> nameList = new ArrayList<>(List.of(getColumnsNames()));
            List<Integer> indexesToRemove = new ArrayList<>();
            for (int i = 0; i<classList.size(); i++){
                if ((classList.get(i).equals(StringValue.class) || classList.get(i).equals(DateTimeValue.class)) && !colNames.contains(nameList.get(i))){
                    indexesToRemove.add(i);
                }
            }
            for (int i = indexesToRemove.size()-1; i>=0; i--){
                nameList.remove((int)indexesToRemove.get(i));
                classList.remove((int)indexesToRemove.get(i));
            }
            String[] columnNames = nameList.toArray(String[]::new);
            Class[] types = classList.toArray(Class[]::new);
            DataFrame result = new DataFrame(columnNames, types);
            for (var values : map.keySet()) {
                List<Value> toAdd = new ArrayList<>(values);
                DataFrame dataFrameHelp = map.get(values);

                for (var column : dataFrameHelp.columns) {
                    if(colNames.contains(column.getName())){
                        continue;
                    }
                    Value sum = column.list.get(0);
                    Value zero = column.list.get(0);
                    for (var value : column.list) {
                        sum = sum.add(value);
                    }
                    sum = sum.sub(zero);
                    Value mean = sum.div(new IntegerValue(column.list.size()));

                    if(column.getType().equals(IntegerValue.class)) {
                        IntegerValue output = new IntegerValue(0);
                        IntegerValue powVal = new IntegerValue(2);
                        for (var value: column.list) {
                            output = output.add(value.sub(mean).pow(powVal));
                        }
                        output = output.div((new IntegerValue(column.list.size())));
                        int helpOutput = (int) Math.sqrt(Double.parseDouble(output.toString()));
                        IntegerValue std = new IntegerValue(helpOutput);
                        toAdd.add(std);
                    }
                    else if(column.getType().equals(FloatValue.class)) {
                        FloatValue output = new FloatValue((float)0.0);
                        FloatValue powVal = new FloatValue((float)2.0);
                        for (var value: column.list) {
                            output = output.add(value.sub(mean).pow(powVal));
                        }
                        output = output.div((new IntegerValue(column.list.size())));
                        float helpOutput = (float) Math.sqrt(Double.parseDouble(output.toString()));
                        FloatValue std = new FloatValue(helpOutput);
                        toAdd.add(std);
                    }
                    else if(column.getType().equals(DoubleValue.class)) {
                        DoubleValue output = new DoubleValue(0.0);
                        DoubleValue powVal = new DoubleValue(2.0);
                        for (var value: column.list) {
                            output = output.add(value.sub(mean).pow(powVal));
                        }
                        output = output.div((new IntegerValue(column.list.size())));
                        double helpOutput = Math.sqrt(Double.parseDouble(output.toString()));
                        DoubleValue std = new DoubleValue(helpOutput);
                        toAdd.add(std);
                    }
                    else  {
                        Value output = column.elAtIndex(0);
                        toAdd.add(output);
                    }
                }
                try{
                    result.addRow(toAdd);
                }
                catch (IOException e){
                    System.out.println("Adding elements to DataFrame unsuccessful");
                }
            }
            return result;
        }

        /**
         * variance is calculeted by [(a_0 - mean)^2 + (a_1 -mean)^2 + ...]/n where n=number of elements
         * @return new dataframe which has only one column with var values for each value which original dataframe is sorted by eg. "id"
         */
        @Override
        public DataFrame var() throws WrongTypeInColumnException {
            List<Class<? extends Value>> classList = new ArrayList<>(List.of(getColumnsTypes()));
            ArrayList<String> nameList = new ArrayList<>(List.of(getColumnsNames()));
            List<Integer> indexesToRemove = new ArrayList<>();
            for (int i = 0; i<classList.size(); i++){
                if ((classList.get(i).equals(StringValue.class) || classList.get(i).equals(DateTimeValue.class)) && !colNames.contains(nameList.get(i))){
                    indexesToRemove.add(i);
                }
            }
            for (int i = indexesToRemove.size()-1; i>=0; i--){
                nameList.remove((int)indexesToRemove.get(i));
                classList.remove((int)indexesToRemove.get(i));
            }
            String[] columnNames = nameList.toArray(String[]::new);
            Class[] types = classList.toArray(Class[]::new);
            DataFrame result = new DataFrame(columnNames, types);
            for (var values : map.keySet()) {
                List<Value> toAdd = new ArrayList<>(values);
                DataFrame dataFrameHelp = map.get(values);

                for (var column : dataFrameHelp.columns) {
                    if(colNames.contains(column.getName())){
                        continue;
                    }
                    Value sum = column.list.get(0);
                    Value zero = column.list.get(0);
                    for (var value : column.list) {
                        sum = sum.add(value);
                    }
                    sum = sum.sub(zero);
                    Value mean = sum.div(new IntegerValue(column.list.size()));

                    if(column.getType().equals(IntegerValue.class)) {
                        IntegerValue output = new IntegerValue(0);
                        IntegerValue powVal = new IntegerValue(2);
                        for (var value: column.list) {
                            output = output.add(value.sub(mean).pow(powVal));
                        }
                        output = output.div((new IntegerValue(column.list.size())));
                        toAdd.add(output);
                    }
                    else if(column.getType().equals(FloatValue.class)) {
                        FloatValue output = new FloatValue((float)0.0);
                        FloatValue powVal = new FloatValue((float)2.0);
                        for (var value: column.list) {
                            output = output.add(value.sub(mean).pow(powVal));
                        }
                        output = output.div((new IntegerValue(column.list.size())));
                        toAdd.add(output);
                    }
                    else if(column.getType().equals(DoubleValue.class)) {
                        DoubleValue output = new DoubleValue(0.0);
                        DoubleValue powVal = new DoubleValue(2.0);
                        for (var value: column.list) {
                            output = output.add(value.sub(mean).pow(powVal));
                        }
                        output = output.div((new IntegerValue(column.list.size())));
                        toAdd.add(output);
                    }
                    else  {
                        Value output = column.elAtIndex(0);
                        toAdd.add(output);
                    }
                }
                try{
                    result.addRow(toAdd);
                }
                catch (IOException e){
                    System.out.println("Adding elements to DataFrame unsuccessful");
                }
            }
            return result;
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
     * implementing Groupby interface methods
     * making hash map with keys given by @param groupBy and values which are new DataFrames: smaller than original one and
     * having specific property eg. when grupBy "id" first DataFrame will have columns with only "id" == "a"
     * @param groupBy name of column to sort for
     * @return inner class having smaller data frames which are sorted by the column given in @param groupBy
     *
     */
    public DataFrameGroupBy grupby(String[] groupBy) throws WrongTypeInColumnException {
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