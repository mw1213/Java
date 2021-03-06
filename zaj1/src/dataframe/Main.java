package dataframe;

import dataFrameDB.DataFrameDB;
import dataFrameThreads.DataFrameThreads;
import myExceptions.AddingWrongClassesException;
import myExceptions.WrongTypeInColumnException;
import value.DateTimeValue;
import value.DoubleValue;
import value.FloatValue;
import value.StringValue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class Main {
    public static void main(String[] args) {

/*        DataFrame df = new DataFrame(new String[]{"col1","col2"}, new Class[]{IntegerValue.class, IntegerValue.class});
        df.addRow(new IntegerValue(1), new IntegerValue(2));
        df.addRow(new IntegerValue(3), new IntegerValue(4));
        df.addRow(new IntegerValue(12), new IntegerValue(212));
        df.addRow(new IntegerValue(12), new IntegerValue(2123));

        DataFrame df2 = df.get(new String[]{"col1","col2"}, true);
        DataFrame df3 = df.get(new String[]{"col1","col1"}, false);
        DataFrame df4 = df.get(new String[]{"col1","col2"}, true);
        System.out.println(" -- DataFrame1 -- \n");
        System.out.println(df);
        System.out.println(" -- DataFrame2 -- \n");
        System.out.println(df2);
        System.out.println(" -- DataFrame3 -- \n");
        System.out.println(df3);
        System.out.println(" -- DataFrame4 -- \n");
        System.out.println(df4);
        System.out.println("Porównanie czy skopiowane df są równe df1:");
        System.out.println(" " + df2.equals(df) + " -+- " + df3.equals(df)+ " -+- " + df4.equals(df) + "\n");

        System.out.println(" -- DataFrame z wiersza [2] -- \n");
        System.out.println(df.iloc(2));
        System.out.println(" -- DataFrame z wierszy [4,14] -- \n");
        System.out.println(df.iloc(4, 14));
        //DataFrame read = new DataFrame("/home/maciej/IdeaProjects/Java/zaj1/src/dataframe/data.csv", new Class[]{DoubleValue.class, DoubleValue.class, DoubleValue.class}, true);
        //DataFrame read = new DataFrame("X:\\maciejw\\java_course\\zaj1\\src\\dataframe\\data.csv", new Class[]{DoubleValue.class, DoubleValue.class, DoubleValue.class}, true);
        //DataFrame read = new DataFrame("C:\\Users\\Maciej Wilk\\IdeaProjects\\java_course\\Java\\zaj1\\src\\dataframe\\data.csv", new Class[]{DoubleValue.class, DoubleValue.class, DoubleValue.class}, true);
        //System.out.println(read);

        System.out.println(df.get("col1"));
*/
/*
        try {
            DataFrame read2 = new DataFrame("C:\\Users\\Maciej Wilk\\IdeaProjects\\java_course\\Java\\zaj1\\src\\groupby\\groupby.csv", new Class[]{StringValue.class, DateTimeValue.class, FloatValue.class, FloatValue.class}, true);
            //DataFrame read3 = new DataFrame("C:\\Users\\Maciej Wilk\\IdeaProjects\\java_course\\Java\\zaj1\\src\\groupby\\groubymulti.csv", new Class[]{StringValue.class, DateTimeValue.class, DoubleValue.class, DoubleValue.class}, true);
            //DataFrame read2 = new DataFrame("/home/maciej/IdeaProjects/Java/zaj1/src/groupby/groupby.csv", new Class[]{StringValue.class, DateTimeValue.class, FloatValue.class, FloatValue.class}, true);
            //DataFrame read3 = new DataFrame("/home/maciej/IdeaProjects/Java/zaj1/src/groupby/groubymulti.csv", new Class[]{StringValue.class, DateTimeValue.class, DoubleValue.class, DoubleValue.class}, true);

            //System.out.println(read2.grupby(new String[]{"id"}).max());
            System.out.println(read2.grupby(new String[]{"id"}).min());
            //System.out.println(read2.grupby(new String[]{"id"}).sum());
            //System.out.println(read2.grupby(new String[]{"id"}).mean());
            //System.out.println(read2.grupby(new String[]{"id"}).var());
            //System.out.println(read2.grupby(new String[]{"id"}).std());

            //System.out.println(read3.grupby(new String[]{"id", "date"}).max());
            //System.out.println(read3.grupby(new String[]{"id", "date"}).min());
            //System.out.println(read3.grupby(new String[]{"id", "date"}).sum());
            //System.out.println(read3.grupby(new String[]{"id", "date"}).mean());
            //System.out.println(read3.grupby(new String[]{"id", "date"}).var());
            //System.out.println(read3.grupby(new String[]{"id", "date"}).std());

        }
        catch (IOException e){
            System.out.println(e);
        } catch (WrongTypeInColumnException e) {
            e.printMessage();
        }
*/
        DataFrame read3 = null;
        DataFrameThreads read4 = null;
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("/home/maciej/IdeaProjects/Java/zaj1/src/test1.csv"));
            //read4 = new DataFrame("C:\\Users\\Maciej Wilk\\IdeaProjects\\java_course\\Java\\zaj1\\src\\groupby\\groupby.csv",
            //      new Class[]{StringValue.class, DateTimeValue.class, FloatValue.class, FloatValue.class}, true, 20);
            DataFrame dataFrame = new DataFrame("/home/maciej/IdeaProjects/Java/zaj1/large_groupby.csv", new Class[]{StringValue.class, DateTimeValue.class, StringValue.class, DoubleValue.class,DoubleValue.class}, true);



            Long startTime;
            Long endTime;
            Long time;
            Float time1;
            for(int i=0;i<4;i++){
                DataFrame fromData;
                DataFrame fromData2;
                fromData2 = dataFrame.get(new String[]{"id, date, total, val"}, true);
                DataFrameThreads threads;
                fromData = dataFrame.get(new String[]{"id, date, total, val"}, true);
                threads = new DataFrameThreads(fromData2);

                startTime = System.nanoTime();
                fromData.grupby(new String[]{"id"}).min();
                endTime = System.nanoTime();
                time = endTime - startTime;
                time1 = Float.valueOf(time);
                writer.write("DataFrame," + String.valueOf(time1) + ",");
                startTime = System.nanoTime();
                fromData.grupby(new String[]{"id"}).max();
                endTime = System.nanoTime();
                time = endTime - startTime;
                time1 = Float.valueOf(time);
                writer.write(String.valueOf(time1) + ",");
                startTime = System.nanoTime();
                fromData.grupby(new String[]{"id"}).sum();
                endTime = System.nanoTime();
                time = endTime - startTime;
                time1 = Float.valueOf(time);
                writer.write(String.valueOf(time1) + ",");
                startTime = System.nanoTime();
                fromData.grupby(new String[]{"id"}).mean();
                endTime = System.nanoTime();
                time = endTime - startTime;
                time1 = Float.valueOf(time);
                writer.write(String.valueOf(time1) + ",");
                startTime = System.nanoTime();
                fromData.grupby(new String[]{"id"}).var();
                endTime = System.nanoTime();
                time = endTime - startTime;
                time1 = Float.valueOf(time);
                writer.write(String.valueOf(time1) + ",");
                startTime = System.nanoTime();
                fromData.grupby(new String[]{"id"}).std();
                endTime = System.nanoTime();
                time = endTime - startTime;
                time1 = Float.valueOf(time);
                writer.write(String.valueOf(time1));
                writer.newLine();

                startTime = System.nanoTime();
                threads.grupby(new String[]{"id"}).min();
                endTime = System.nanoTime();
                time = endTime - startTime;
                time1 = Float.valueOf(time);
                writer.write("DataFrameThreads," + String.valueOf(time1) + ",");
                startTime = System.nanoTime();
                threads.grupby(new String[]{"id"}).max();
                endTime = System.nanoTime();
                time = endTime - startTime;
                time1 = Float.valueOf(time);
                writer.write(String.valueOf(time1) + ",");
                startTime = System.nanoTime();
                threads.grupby(new String[]{"id"}).sum();
                endTime = System.nanoTime();
                time = endTime - startTime;
                time1 = Float.valueOf(time);
                writer.write(String.valueOf(time1) + ",");
                startTime = System.nanoTime();
                threads.grupby(new String[]{"id"}).mean();
                endTime = System.nanoTime();
                time = endTime - startTime;
                time1 = Float.valueOf(time);
                writer.write(String.valueOf(time1) + ",");
                startTime = System.nanoTime();
                threads.grupby(new String[]{"id"}).var();
                endTime = System.nanoTime();
                time = endTime - startTime;
                time1 = Float.valueOf(time);
                writer.write(String.valueOf(time1) + ",");
                startTime = System.nanoTime();
                threads.grupby(new String[]{"id"}).std();
                endTime = System.nanoTime();
                time = endTime - startTime;
                time1 = Float.valueOf(time);
                writer.write(String.valueOf(time1));
                writer.newLine();
            }
            writer.close();
        } catch (IOException | WrongTypeInColumnException e) {
            e.printStackTrace();
        }

/*        try {
            System.out.println(read4.grupby(new String[]{"id", "total"}).max());
        } catch (WrongTypeInColumnException e) {
            e.printMessage();
        }
*/

/*
        read4.get("total").changeElementToWrong(new StringValue("Dane_zmienione"),4);
        //System.out.println(read4.toString());
        try {
            System.out.println(read4.grupby(new String[]{"id"}).max());
        } catch (WrongTypeInColumnException e) {
            e.printMessage();
        } catch (NumberFormatException e){
            e.printStackTrace();
        }

        DataFrameDB dataFrameDB1 = new DataFrameDB(read4, "read4");
*/

 /*       try {
            DataFrameDB dataFrameDB = new DataFrameDB("jdbc:mysql://mysql.agh.edu.pl/maciejw",
                    "maciejw", "dEgqnqsG8ripMcK3");

            DataFrame fromDataFrameDB;
            fromDataFrameDB = DataFrameDB.createDataFrameFromDataFrameDB("jdbc:mysql://mysql.agh.edu.pl/maciejw",
                    "maciejw", "dEgqnqsG8ripMcK3", "SELECT id, total FROM SmallRead");
            System.out.println(fromDataFrameDB.iloc(0, 50));

            System.out.println(dataFrameDB.getMin("SmallRead"));

            System.out.print(dataFrameDB.grupby("SmallRead", new String[]{"id","total"}).mean());

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (AddingWrongClassesException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (WrongTypeInColumnException e) {
            e.printStackTrace();
        }
*/

    }
}