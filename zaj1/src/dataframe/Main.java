package dataframe;

import value.DoubleValue;
import value.IntegerValue;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        DataFrame df = new DataFrame(new String[]{"col1","col2"}, new Class[]{IntegerValue.class, IntegerValue.class});
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
        //DataFrame read = new DataFrame("X:\\maciejw\\java_course\\zaj1\\src\\dataframe\\data.csv", new Class[]{DoubleValue.class, DoubleValue.class, DoubleValue.class}, true);
        DataFrame read = new DataFrame("C:\\Users\\Maciej Wilk\\IdeaProjects\\java_course\\zaj1\\src\\dataframe\\data.csv", new Class[]{DoubleValue.class, DoubleValue.class, DoubleValue.class}, true);
        System.out.println(read);

        System.out.println(df.get("col1"));
    }
}