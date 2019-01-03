package dataFrameThreads;


import dataframe.Column;
import dataframe.DataFrame;
import myExceptions.WrongTypeInColumnException;
import value.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class DataFrameThreads extends DataFrame {


        static DataFrame counted;
        static DataFrame given;
        static List<Value> keys;
        static String func = "";

        public DataFrameThreads() {
            super();
        }

        public DataFrameThreads(String[] colsInput, Class<? extends Value>[] typesInput) {
            super(colsInput, typesInput);
        }

        public DataFrameThreads(List<String> names, List<Class<? extends Value>> types) {
            columns = new ArrayList<>();
            for (int i=0; i<types.size(); i++){
                if (i >= names.size()) break;

                if(isUnique(names.get(i))) {
                    columns.add(new Column(names.get(i), types.get(i)));
                }
            }
        }

        public DataFrameThreads(String address, Class<? extends Value>[] typesInput, boolean header) throws IOException, WrongTypeInColumnException {
            super(address, typesInput, header);
        }

        @Override
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

        public class DataMapThread extends  DataFrame.DataFrameGroupBy{


            /**
             * constructor for inner class
             *
             * @param _map      gives map to store in this.map
             * @param _colNames gives colNames to store in this.colNames
             */
            public DataMapThread(HashMap<List<Value>, DataFrame> _map, String[] _colNames) {
                super(_map, _colNames);
            }

            public DataFrame max() {
                func = "max";
                return uni();
            }

            public DataFrame min() {
                func = "min";
                return uni();
            }

            public DataFrame mean() {
                func = "mean";
                return uni();
            }

            public DataFrame var() {
                func = "var";
                return uni();
            }

            public DataFrame std() {
                func = "std";
                return uni();
            }

            public DataFrame sum() {
                func = "sum";
                return uni();
            }


            public DataFrame uni() {
                int iterator = 0;
                for (Map.Entry<List<Value>, DataFrame> entry : map.entrySet()) {
                    if (iterator == 0) {
                        if (func.equals("max") || func.equals("min"))
                            counted = entry.getValue().emptyDataFrame();
                        else
                            counted = entry.getValue().emptyWithoutData();
                    }
                    Threading thread=new Threading();
                    thread.init(entry.getKey(), entry.getValue());
                    thread.start();
                    iterator++;
                }
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return counted;
            }

            public Map<List<Value>, DataFrame> returnMap(){
                return this.map;
            }

        }

        public class Threading extends Thread{
            private List<Value> keyed;
            private DataFrame given;

            public void init(List<Value> keys, DataFrame give){
                this.keyed=keys;
                this.given=give;
            }

            @Override
            public void run(){
                if (func.equals("max")) {
                    try {
                        maxing(keyed, given);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (WrongTypeInColumnException e) {
                        e.printStackTrace();
                    }
                }
                else if (func.equals("min")) {
                    try {
                        mining(keyed, given);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (WrongTypeInColumnException e) {
                        e.printMessage();
                    }
                }
                else if (func.equals("mean")) {
                    try {
                        meaning(keyed, given);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (WrongTypeInColumnException e) {
                        e.printMessage();
                    }
                }
                else if (func.equals("sum")) {
                    try {
                        suming(keyed, given);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (WrongTypeInColumnException e) {
                        e.printMessage();
                    }
                }
                else if (func.equals("var")) {
                    try {
                        varing(keyed, given);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (WrongTypeInColumnException e) {
                        e.printStackTrace();
                    }
                }
                else if (func.equals("std")) {
                    try {
                        stding(keyed, given);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (WrongTypeInColumnException e) {
                        e.printMessage();
                    }
                }
            }
        }

        public synchronized void add(DataFrame toAdd) throws IOException, WrongTypeInColumnException {
            counted.addRow((List<Value>) toAdd);
        }

        public void maxing(List<Value> keyed, DataFrame dated) throws IOException, WrongTypeInColumnException {
            DataFrame computed = counted.emptyDataFrame();
            Value[] input = new Value[dated.columns.size()];
            int iterator = 0;
            for (var column : dated.columns) {
                Value max = column.list.get(0);
                for (int i = 0; i < column.list.size(); i++) {
                    var value = column.elAtIndex(i);

                    if (value.getClass().equals(column.getType())) {
                        if (value.gte(max)) max = value;
                    } else {
                        throw new WrongTypeInColumnException(i, column.getType(), value.getClass(), column.getName());
                    }
                }
                if (keyed.size() > iterator)
                    input[iterator] = keyed.get(iterator);
                else
                    input[iterator] = max;
                iterator++;
                if (iterator == dated.columns.size()) {
                    iterator = 0;
                    computed.addElement(input);
                }
            }
            counted.addRow((List<Value>) computed);
        }

        public void mining(List<Value> keyed, DataFrame dated) throws IOException, WrongTypeInColumnException {
                DataFrame computed = counted.emptyDataFrame();
                Value[] input = new Value[dated.columns.size()];
                int iterator = 0;
                for (var column : dated.columns) {
                    Value min = column.list.get(0);
                    for (int i = 0; i < column.list.size(); i++) {
                        var value = column.elAtIndex(i);

                        if (value.getClass().equals(column.getType())) {
                            if (value.lte(min)) min = value;
                        } else {
                            throw new WrongTypeInColumnException(i, column.getType(), value.getClass(), column.getName());
                        }
                    }
                    if (keyed.size() > iterator)
                        input[iterator] = keyed.get(iterator);
                    else
                        input[iterator] = min;
                    iterator++;
                    if (iterator == dated.columns.size()) {
                        iterator = 0;
                        computed.addElement(input);
                    }
                }
            counted.addRow((List<Value>) computed);
        }

        public void meaning(List<Value> keyed, DataFrame dated) throws IOException, WrongTypeInColumnException {
            Value[] input = new Value[dated.columns.size()];
            Value x = null;
            int iterator = 0;
            List<Class<? extends Value>> classList = new ArrayList<>(List.of(getColumnsTypes()));
            ArrayList<String> nameList = new ArrayList<>(List.of(getColumnsNames()));
            List<Integer> indexesToRemove = new ArrayList<>();
            for (int i = 0; i<classList.size(); i++){
                if ((classList.get(i).equals(StringValue.class) || classList.get(i).equals(DateTimeValue.class))){
                    indexesToRemove.add(i);
                }
            }
            for (int i = indexesToRemove.size()-1; i>=0; i--){
                nameList.remove((int)indexesToRemove.get(i));
                classList.remove((int)indexesToRemove.get(i));
            }
            String[] columnNames = nameList.toArray(String[]::new);
            Class[] types = classList.toArray(Class[]::new);
            DataFrame computed = new DataFrame(columnNames, types);

            for (var column : dated.columns) {
                Value sum = column.list.get(0);
                Value zero = column.list.get(0);
                for (var value : column.list) {
                    sum = sum.add(value);
                }
                sum = sum.sub(zero);
                Value mean = sum.div(new IntegerValue(column.list.size()));
                if (keyed.size() > iterator)
                    input[iterator] = keyed.get(iterator);
                else
                    input[iterator] = mean;
                iterator++;
                if (iterator == dated.columns.size()) {
                    iterator = 0;
                    computed.addElement(input);
                }
            }
            counted.addRow((List<Value>) computed);
        }

        public void suming(List<Value> keyed, DataFrame dated) throws IOException, WrongTypeInColumnException {
            Value[] input = new Value[dated.columns.size()];
            int iterator = 0;
            List<Class<? extends Value>> classList = new ArrayList<>(List.of(getColumnsTypes()));
            ArrayList<String> nameList = new ArrayList<>(List.of(getColumnsNames()));
            List<Integer> indexesToRemove = new ArrayList<>();
            for (int i = 0; i<classList.size(); i++){
                if ((classList.get(i).equals(StringValue.class) || classList.get(i).equals(DateTimeValue.class))){
                    indexesToRemove.add(i);
                }
            }
            for (int i = indexesToRemove.size()-1; i>=0; i--){
                nameList.remove((int)indexesToRemove.get(i));
                classList.remove((int)indexesToRemove.get(i));
            }
            String[] columnNames = nameList.toArray(String[]::new);
            Class[] types = classList.toArray(Class[]::new);
            DataFrame computed = new DataFrame(columnNames, types);

            for (var column: dated.columns) {
                if (!column.getClass().equals(DateTimeValue.class)) {
                    Value sum = column.list.get(0);
                    Value zero = column.list.get(0);
                    for (var value : column.list) {
                        sum = sum.add(value);
                    }
                    sum = sum.sub(zero);
                    if (keyed.size() > iterator)
                        input[iterator] = keyed.get(iterator);
                    else
                        input[iterator] = sum;
                    iterator++;
                    if (iterator == dated.columns.size()) {
                        iterator = 0;
                        computed.addElement(input);
                    }
                }
            }

            counted.addRow((List<Value>) computed);
        }


        public void varing(List<Value> keyed, DataFrame dated) throws IOException, WrongTypeInColumnException {
            Value[] input = new Value[dated.columns.size()];
            int iterator = 0;
            List<Class<? extends Value>> classList = new ArrayList<>(List.of(getColumnsTypes()));
            ArrayList<String> nameList = new ArrayList<>(List.of(getColumnsNames()));
            List<Integer> indexesToRemove = new ArrayList<>();
            for (int i = 0; i<classList.size(); i++){
                if ((classList.get(i).equals(StringValue.class) || classList.get(i).equals(DateTimeValue.class))){
                    indexesToRemove.add(i);
                }
            }
            for (int i = indexesToRemove.size()-1; i>=0; i--){
                nameList.remove((int)indexesToRemove.get(i));
                classList.remove((int)indexesToRemove.get(i));
            }
            String[] columnNames = nameList.toArray(String[]::new);
            Class[] types = classList.toArray(Class[]::new);
            DataFrame computed = new DataFrame(columnNames, types);

            for (var column : dated.columns) {
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
                    if (keyed.size() > iterator)
                        input[iterator] = keyed.get(iterator);
                    else
                        input[iterator] = output;
                    iterator++;
                    if (iterator == dated.columns.size()) {
                        iterator = 0;
                        computed.addElement(input);
                    }
                }
                else if(column.getType().equals(FloatValue.class)) {
                    FloatValue output = new FloatValue((float)0.0);
                    FloatValue powVal = new FloatValue((float)2.0);
                    for (var value: column.list) {
                        output = output.add(value.sub(mean).pow(powVal));
                    }
                    output = output.div((new IntegerValue(column.list.size())));
                    if (keyed.size() > iterator)
                        input[iterator] = keyed.get(iterator);
                    else
                        input[iterator] = output;
                    iterator++;
                    if (iterator == dated.columns.size()) {
                        iterator = 0;
                        computed.addElement(input);
                    }
                }
                else if(column.getType().equals(DoubleValue.class)) {
                    DoubleValue output = new DoubleValue(0.0);
                    DoubleValue powVal = new DoubleValue(2.0);
                    for (var value: column.list) {
                        output = output.add(value.sub(mean).pow(powVal));
                    }
                    output = output.div((new IntegerValue(column.list.size())));
                    if (keyed.size() > iterator)
                        input[iterator] = keyed.get(iterator);
                    else
                        input[iterator] = output;
                    iterator++;
                    if (iterator == dated.columns.size()) {
                        iterator = 0;
                        computed.addElement(input);
                    }
                }
                else  {
                    Value output = column.elAtIndex(0);
                    if (keyed.size() > iterator)
                        input[iterator] = keyed.get(iterator);
                    else
                        input[iterator] = output;
                    iterator++;
                    if (iterator == dated.columns.size()) {
                        iterator = 0;
                        computed.addElement(input);
                    }
                }
            }


            counted.addRow((List<Value>) computed);
        }

        public void stding(List<Value> keyed, DataFrame dated) throws IOException, WrongTypeInColumnException {
            Value[] input = new Value[dated.columns.size()];
            int iterator = 0;
            List<Class<? extends Value>> classList = new ArrayList<>(List.of(getColumnsTypes()));
            ArrayList<String> nameList = new ArrayList<>(List.of(getColumnsNames()));
            List<Integer> indexesToRemove = new ArrayList<>();
            for (int i = 0; i<classList.size(); i++){
                if ((classList.get(i).equals(StringValue.class) || classList.get(i).equals(DateTimeValue.class))){
                    indexesToRemove.add(i);
                }
            }
            for (int i = indexesToRemove.size()-1; i>=0; i--){
                nameList.remove((int)indexesToRemove.get(i));
                classList.remove((int)indexesToRemove.get(i));
            }
            String[] columnNames = nameList.toArray(String[]::new);
            Class[] types = classList.toArray(Class[]::new);
            DataFrame computed = new DataFrame(columnNames, types);

            for (var column : dated.columns) {
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
                    if (keyed.size() > iterator)
                        input[iterator] = keyed.get(iterator);
                    else
                        input[iterator] = std;
                    iterator++;
                    if (iterator == dated.columns.size()) {
                        iterator = 0;
                        computed.addElement(input);
                    }
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
                    if (keyed.size() > iterator)
                        input[iterator] = keyed.get(iterator);
                    else
                        input[iterator] = std;
                    iterator++;
                    if (iterator == dated.columns.size()) {
                        iterator = 0;
                        computed.addElement(input);
                    }
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
                    if (keyed.size() > iterator)
                        input[iterator] = keyed.get(iterator);
                    else
                        input[iterator] = std;
                    iterator++;
                    if (iterator == dated.columns.size()) {
                        iterator = 0;
                        computed.addElement(input);
                    }
                }
                else  {
                    Value output = column.elAtIndex(0);
                    if (keyed.size() > iterator)
                        input[iterator] = keyed.get(iterator);
                    else
                        input[iterator] = output;
                    iterator++;
                    if (iterator == dated.columns.size()) {
                        iterator = 0;
                        computed.addElement(input);
                    }
                }
            }
            counted.addRow((List<Value>) computed);
        }

}
