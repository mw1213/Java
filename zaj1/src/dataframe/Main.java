package dataframe;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        DataFrame df = new DataFrame(new String[] {"kol1", "kol2", "kol3"}, new String[]{"int","double","String"});
        df.addRow(1, 12.03, "wiersz1");
        df.addRow(2, 26.12, "wiersz2");
        df.addRow(3,12.11,"wiersz3");
        df.addRow(4,12.22,"wiersz4");
        df.addRow(5,333.33,"wiersz5");
        df.addRow(6,1212.12,"wiersz6");
        df.addRow(7,123.123,"wiersz7");
        df.addRow(8,12.3123,"wiersz8");
        DataFrame df2 = df.get(new String[]{"kol1","kol2", "kol3"}, true);
        DataFrame df3 = df.get(new String[]{"kol1","kol3", "kol1"}, false);
        DataFrame df4 = df.get(new String[]{"kol1","kol2"}, true);
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
//
        DataFrame read = new DataFrame("X:\\maciejw\\java_course\\zaj1\\src\\dataframe\\data.csv", new String[] {"Double", "Double", "Double"}, true);
        System.out.println(read);

    }
}