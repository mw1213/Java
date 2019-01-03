package dataFrameDB;

import dataframe.*;
import groupby.Applyable;
import myExceptions.AddingWrongClassesException;
import myExceptions.WrongTypeInColumnException;
import value.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class DataFrameDB extends DataFrame {

    private Connection conn = null;
    private Statement stmt = null;
    private ResultSet rs = null;
    private String address,user,password;
    private String name;


    public DataFrameDB (String address, String user, String password){
        this.address=address;
        this.user=user;
        this.password=password;
        connect();
    }
    public DataFrameDB (DataFrame dataFrame, String name, String address, String user, String password) throws SQLException{
        this.address=address;
        this.user=user;
        this.password=password;
        this.name=name;
        connect();
        DatabaseMetaData dbm = conn.getMetaData();
        ResultSet rs = dbm.getTables(null, null, name, null);
        if (!rs.next()) {
            String[] types = new String[dataFrame.width()];
            String type;
            for (int i = 0; i < dataFrame.width(); i++) {
                type = dataFrame.get(i).getType().toString();
                if (type.contains("Integer")) types[i] = "INT";
                else if (type.contains("Float")) types[i] = "FLOAT";
                else if (type.contains("Double")) types[i] = "DOUBLE";
                else if (type.contains("Date")) types[i] = "DATE";
                else types[i] = "TEXT";
            }
            createTable(name, dataFrame.getColumnsNames(), types);

        }
        String[] values= new String[dataFrame.width()];
        String query = "SELECT * FROM "+name+";";
        stmt=conn.createStatement();
        rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        int j=0,temp=1000;
        if (temp>=dataFrame.size()) temp=dataFrame.size();
        for (int m=0;m<1000;m++){
            query="INSERT INTO "+name+" VALUES ";
            for (; j<temp;j++){
                for (int k=0;k<dataFrame.width();k++){
                    values[k]=dataFrame.get(k).elAtIndex(j).toString();
                }
                query+=valuesToAdd(rsmd,values);
                //System.out.println(j);
                if (j!=temp-1) query+=", \n";
            }
            temp+=1000;
            if (temp>=dataFrame.size()) temp=dataFrame.size();
            query+=";";
            //System.out.println(j);
            //System.out.println(query);
            if(!query.equals("INSERT INTO "+name+" VALUES ;")){
                stmt.executeUpdate(query);
            }
        }

    }
    public void createTable (String name, String[] colNames, String[] types) throws SQLException{
        //connect();
        stmt = conn.createStatement();
        String query = "CREATE TABLE IF NOT EXISTS "+name+ "(";
        for (int i=0;i<colNames.length;i++){
            query = query + colNames[i] + " " + types[i];
            if (i!=colNames.length-1) query = query + ", ";
        }
        query = query + ");";
        System.out.println(query);
        stmt.executeUpdate(query);
    }
    public void addRowsToTable (String name, String[] values) throws SQLException{
        //connect();
        String query = "SELECT * FROM "+name+";";
        rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        int width = rsmd.getColumnCount();
        if (values.length==width){
            query = "INSERT INTO "+name+" VALUES ";
            query+=valuesToAdd(rsmd,values);
            query+=";";
            System.out.println(query);
            stmt.executeUpdate(query);
        }
    }
    public String valuesToAdd(ResultSetMetaData rsmd,String...values) throws SQLException{
        String result="(";
        for (int i=0;i<values.length;i++){
            if (rsmd.getColumnTypeName(i+1).contains("CHAR") || rsmd.getColumnTypeName(i+1).contains("DATE")){
                result=result+"'"+values[i]+"'";
            }
            else result=result+values[i];
            if (i!=values.length-1) result=result+", ";
        }
        result+=")";
        return result;
    }

    public void connect() {
        for (int i=0; i<3; i++){
            try {
                Class.forName("com.mysql.jdbc.Driver").newInstance();
                conn =
                        DriverManager.getConnection(address,
                                user, password);
                if (conn != null) {
                    break;
                }
            } catch (SQLException ex){

                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
            } catch (Exception e) {

                e.printStackTrace();
            }
        }
        if (conn != null){
            System.out.println("Connected succesfuly!");
        }
        else {
            System.out.println("Couldn't connect to database :(");
        }

    }

    public DataFrame getDataFrame (String name) throws SQLException, IllegalAccessException, InvocationTargetException,
            NoSuchMethodException, WrongTypeInColumnException, InstantiationException, AddingWrongClassesException {
        this.name=name;
        String query = "SELECT * FROM "+name;
        return getDataFrameFromQuery(query);
    }
    public String[] getNameListFromDB (String name) throws SQLException{
        stmt = conn.createStatement();
        String query = "SELECT * FROM "+name+" LIMIT 1";
        rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        int width = rsmd.getColumnCount();
        String [] names = new String[width];
        for (int i=0;i<width;i++){
            names[i] =rsmd.getColumnName(i+1);
        }
        return names;
    }
    public Class<? extends Value>[] getTypesFromDB (String name) throws SQLException{
        stmt = conn.createStatement();
        String query = "SELECT * FROM "+name+" LIMIT 1";
        rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        Class<? extends Value>[] types = new Class[rsmd.getColumnCount()];
        for (int j=0;j<rsmd.getColumnCount();j++){
            String type = rsmd.getColumnTypeName(j+1);
            if (type.contains("INT")) types[j] = IntegerValue.class;
            else if(type.contains("FLOAT")) types[j]= FloatValue.class;
            else if(type.contains("DOUBLE") || type.contains("DECIMAL")) types[j]= DoubleValue.class;
            else if (type.contains("DATE") || type.contains("TIME") ||
                    type.contains("YEAR")) types[j]= DateTimeValue.class;
            else types[j]=StringValue.class;
        }
        return types;
    }
    public DataFrame getDataFrameFromQuery (String query) throws SQLException, IllegalAccessException, InvocationTargetException,
            NoSuchMethodException, WrongTypeInColumnException, InstantiationException, AddingWrongClassesException {
        stmt = conn.createStatement();
        rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        int width = rsmd.getColumnCount();
        String [] names = new String[width];
        for (int i=0;i<width;i++){
            names[i] =rsmd.getColumnName(i+1);
        }
        Class<? extends Value>[] types = new Class[width];
        for (int j=0;j<width;j++){
            String type = rsmd.getColumnTypeName(j+1);
            if (type.contains("INT")) types[j] = IntegerValue.class;
            else if(type.contains("FLOAT")) types[j]= FloatValue.class;
            else if(type.contains("DOUBLE") || type.contains("DECIMAL")) types[j]= DoubleValue.class;
            else if (type.contains("DATE") || type.contains("TIME") ||
                    type.contains("YEAR")) types[j]=DateTimeValue.class;
            else types[j]=StringValue.class;
        }
        DataFrame result=new DataFrame(names,types);
        Value[] values = new Value[width];
        List<Constructor<? extends Value>> constructors = new ArrayList<>(types.length);
        for (int i = 0; i < types.length; i++) {
            constructors.add(types[i].getConstructor(String.class));
        }
        int ind=0;
        while (rs.next()){
            for (int j = 0; j < width; j++) {
                values[j] = constructors.get(j).newInstance(rs.getString(j+1));
                if (!values[j].getSet())
                    throw new WrongTypeInColumnException(ind, constructors.get(j).getDeclaringClass(), values[j].getClass(), names[j]);
                ind++;
            }
            addRowToDF(width,result,values);
        }
        return result;
    }

    public DataFrame addRowToDF(int width,DataFrame result,Value...values) throws WrongTypeInColumnException, AddingWrongClassesException {
        if(width!=values.length){
            System.out.println("Nie podano wszystkich argumentów!");
            return result;
        }
        for (int i=0;i<values.length;i++){
            if(!result.get(i).checkElement(values[i])) {
                int a=result.get(0).size();
                if(i==0){
                    a=-1;
                    if(a<0) a=0;
                }
                throw new WrongTypeInColumnException(a, get(i).getType(), values[i].getClass(), get(i).getName());
            }
        }
        for (int j=0;j<values.length;j++){
            result.get(j).addElement(values[j]);
        }
        return result;
    }
    public void dropTable (String name) throws SQLException{
        stmt = conn.createStatement();
        String query = "DROP TABLE "+name+";";
        stmt.executeUpdate(query);
    }

    public static DataFrame createDataFrameFromDataFrameDB(String address, String user, String password,  String query)throws SQLException, IllegalAccessException, InvocationTargetException,
            NoSuchMethodException, WrongTypeInColumnException, InstantiationException, AddingWrongClassesException {
        Connection conn =null;
        Statement stmt;
        ResultSet rs;

        for (int i=0; i<3; i++){
            try {
                Class.forName("com.mysql.jdbc.Driver").newInstance();
                conn =
                        DriverManager.getConnection(address,
                                user, password);
                if (conn != null) {
                    break;
                }
            } catch (SQLException ex){

                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
            } catch (Exception e) {

                e.printStackTrace();
            }
        }
        if (conn != null){
            System.out.println("Connected succesfuly!");
        }
        else {
            System.out.println("Couldn't connect to database :(");
        }
        stmt = conn.createStatement();
        rs = stmt.executeQuery(query);
        ResultSetMetaData rsmd = rs.getMetaData();
        int width = rsmd.getColumnCount();
        String [] names = new String[width];
        for (int i=0;i<width;i++){
            names[i] =rsmd.getColumnName(i+1);
        }
        Class<? extends Value>[] types = new Class[width];
        for (int j=0;j<width;j++){
            String type = rsmd.getColumnTypeName(j+1);
            if (type.contains("INT")) types[j] = IntegerValue.class;
            else if(type.contains("FLOAT")) types[j]= FloatValue.class;
            else if(type.contains("DOUBLE") || type.contains("DECIMAL")) types[j]= DoubleValue.class;
            else if (type.contains("DATE") || type.contains("TIME") ||
                    type.contains("YEAR")) types[j]=DateTimeValue.class;
            else types[j]=StringValue.class;
        }
        DataFrame result=new DataFrame(names,types);
        Value[] values = new Value[width];
        List<Constructor<? extends Value>> constructors = new ArrayList<>(types.length);
        for (int i = 0; i < types.length; i++) {
            constructors.add(types[i].getConstructor(String.class));
        }
        int ind=0;
        while (rs.next()){
            for (int j = 0; j < width; j++) {
                values[j] = constructors.get(j).newInstance(rs.getString(j+1));
                if (!values[j].getSet())
                    throw new WrongTypeInColumnException(ind, constructors.get(j).getDeclaringClass(), values[j].getClass(), names[j]);
                ind++;
            }
            for (int i=0;i<values.length;i++){
                if(!result.get(i).checkElement(values[i])) {
                    int a=result.get(0).size();
                    if(i==0){
                        a=-1;
                        if(a<0) a=0;
                    }
                    throw new WrongTypeInColumnException(a, constructors.get(i).getDeclaringClass(), values[i].getClass(), names[i]);
                }
            }
            for (int j=0;j<values.length;j++){
                result.get(j).addElement(values[j]);
            }
        }
        return result;
    }


    public String getOp (String name,String op) throws SQLException{
        String result="";
        String[] colNames = getNameListFromDB(name);
        String query="SELECT ";
        stmt = conn.createStatement();

        for (int i=0;i<colNames.length;i++){
            query+=op+"("+colNames[i]+")";
            if (i!=colNames.length-1) query+=", ";
        }
        query+=" FROM "+name+";";
        //System.out.println(query);
        rs = stmt.executeQuery(query);
        rs.next();
        for (int j=0;j<colNames.length;j++){
            result+=op+" dla kolumny \""+colNames[j]+"\": "+rs.getString(j+1)+"\n";
        }
        return result;
    }
    public String getMin (String name) throws SQLException{
        return getOp(name,"MIN");
    }
    public String getMax (String name) throws SQLException{
        return getOp(name,"MAX");
    }

    public DataFrame groupby (String name, String...strings) throws SQLException {
        DataFrame df = new DataFrame();
        String[] colNames = getNameListFromDB(name);
        String query="SELECT * FROM "+name+" ORDER BY ";
        stmt = conn.createStatement();
        for (int i=0;i<strings.length;i++){
            query+=strings[i];
            if (i!=strings.length-1) query+=", ";
        }
        try {
            System.out.println(query);
            df = getDataFrameFromQuery(query);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (WrongTypeInColumnException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (AddingWrongClassesException e) {
            e.printStackTrace();
        }

        return df;
    }

    public class DataFrameGroupBy extends DataFrame.DataFrameGroupBy {

        private ArrayList<String> columnNames;
        private ArrayList<String> allNames;
        private Statement statement = null;
        private ResultSet resultSet = null;

        public DataFrameGroupBy(HashMap<List<Value>, DataFrame> map, String[] colNames) {
            super(map, colNames);
            columnNames = new ArrayList<>(List.of(colNames));
        }

        @Override
        public DataFrame max() {
            DataFrame result= null;
            try {
                String[] colNames = getNameListFromDB(name);
                String query="SELECT ";
                for (int i=0;i<colNames.length;i++){
                    query+="max("+colNames[i]+")";
                    if (i!=colNames.length-1) query+=", ";
                }
                query+=" FROM "+name+ " GROUP BY ";
                for (int i = 0; i < columnNames.size(); i++) {
                    query+=(columnNames.get(i));
                    query+=((i == columnNames.size() - 1) ? ";" : ", ");
                }
                System.out.println(query);
                result = getDataFrameFromQuery(query);
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (WrongTypeInColumnException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (AddingWrongClassesException e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public DataFrame min() {
            DataFrame result= null;
            try {
                String[] colNames = getNameListFromDB(name);
                String query="SELECT ";
                for (int i=0;i<colNames.length;i++){
                    query+="min("+colNames[i]+")";
                    if (i!=colNames.length-1) query+=", ";
                }
                query+=" FROM "+name+ " GROUP BY ";
                for (int i = 0; i < columnNames.size(); i++) {
                    query+=(columnNames.get(i));
                    query+=((i == columnNames.size() - 1) ? ";" : ", ");
                }
                System.out.println(query);
                result = getDataFrameFromQuery(query);
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (WrongTypeInColumnException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (AddingWrongClassesException e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public DataFrame mean() {
            DataFrame result= null;
            try {
                String[] colNames = getNameListFromDB(name);
                String query="SELECT ";
                for (int i=0;i<colNames.length;i++){
                    query+="avg("+colNames[i]+")";
                    if (i!=colNames.length-1) query+=", ";
                }
                query+=" FROM "+name+ " GROUP BY ";
                for (int i = 0; i < columnNames.size(); i++) {
                    query+=(columnNames.get(i));
                    query+=((i == columnNames.size() - 1) ? ";" : ", ");
                }
                System.out.println(query);
                result = getDataFrameFromQuery(query);
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (WrongTypeInColumnException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (AddingWrongClassesException e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public DataFrame std() {
            DataFrame result= null;
            try {
                String[] colNames = getNameListFromDB(name);
                String query="SELECT ";
                for (int i=0;i<colNames.length;i++){
                    query+="std("+colNames[i]+")";
                    if (i!=colNames.length-1) query+=", ";
                }
                query+=" FROM "+name+ " GROUP BY ";
                for (int i = 0; i < columnNames.size(); i++) {
                    query+=(columnNames.get(i));
                    query+=((i == columnNames.size() - 1) ? ";" : ", ");
                }
                System.out.println(query);
                result = getDataFrameFromQuery(query);
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (WrongTypeInColumnException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (AddingWrongClassesException e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public DataFrame sum() {
            DataFrame result= null;
            try {
                String[] colNames = getNameListFromDB(name);
                String query="SELECT ";
                for (int i=0;i<colNames.length;i++){
                    query+="sum("+colNames[i]+")";
                    if (i!=colNames.length-1) query+=", ";
                }
                query+=" FROM "+name+ " GROUP BY ";
                for (int i = 0; i < columnNames.size(); i++) {
                    query+=(columnNames.get(i));
                    query+=((i == columnNames.size() - 1) ? ";" : ", ");
                }
                System.out.println(query);
                result = getDataFrameFromQuery(query);
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (WrongTypeInColumnException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (AddingWrongClassesException e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public DataFrame var() {
            DataFrame result= null;
            try {
                String[] colNames = getNameListFromDB(name);
                String query="SELECT ";
                for (int i=0;i<colNames.length;i++){
                    query+="variance("+colNames[i]+")";
                    if (i!=colNames.length-1) query+=", ";
                }
                query+=" FROM "+name+ " GROUP BY ";
                for (int i = 0; i < columnNames.size(); i++) {
                    query+=(columnNames.get(i));
                    query+=((i == columnNames.size() - 1) ? ";" : ", ");
                }
                System.out.println(query);
                result = getDataFrameFromQuery(query);
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (WrongTypeInColumnException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (AddingWrongClassesException e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public DataFrame apply(Applyable applyable) {
            return super.apply(applyable);
        }
    }

    @Override
    public DataFrameGroupBy grupby(String[] columnNames) {
        this.name=name;
        return new DataFrameGroupBy(null, columnNames);
    }

    public DataFrameGroupBy grupby(String name ,String[] columnNames) {
        this.name=name;
        return new DataFrameGroupBy(null, columnNames);
    }

}