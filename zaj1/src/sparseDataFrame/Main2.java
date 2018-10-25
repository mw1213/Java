package SparseDataFrame;

import dataframe.*;
import value.IntegerValue;

import java.io.IOException;

public class Main2 {
    public static void main(String[] args) throws IOException {
        SparseDataFrame sdf = new SparseDataFrame(new String[]{"kol1", "kol2", "kol3", "kol4"}, new Class[]{IntegerValue.class, IntegerValue.class, IntegerValue.class} , new IntegerValue(0));
        sdf.addRow(new IntegerValue(1), new IntegerValue(2), new IntegerValue(3), new IntegerValue(11));
        sdf.addRow(new IntegerValue(0), new IntegerValue(0), new IntegerValue(0), new IntegerValue(11));
        sdf.addRow(new IntegerValue(0), new IntegerValue(0), new IntegerValue(0), new IntegerValue(11));
        System.out.println(sdf.printSize());
        sdf.printCOOValue();
        sdf.addRow(new IntegerValue(4), new IntegerValue(5), new IntegerValue(6), new IntegerValue(22));
        sdf.addRow(new IntegerValue(0), new IntegerValue(0), new IntegerValue(0), new IntegerValue(22));
        System.out.println(sdf.printSize());
        sdf.printCOOValue();
        sdf.addRow(new IntegerValue(0), new IntegerValue(0), new IntegerValue(0), new IntegerValue(22));
        sdf.addRow(new IntegerValue(7), new IntegerValue(8), new IntegerValue(9), new IntegerValue(22));
        System.out.println(sdf.printSize());
        sdf.printCOOValue();
        System.out.println(sdf);

        DataFrame df1 = new DataFrame(new String[]{"kl1", "kl2", "kl3"}, new Class[]{IntegerValue.class, IntegerValue.class, IntegerValue.class});
        df1.addRow(new IntegerValue(1),new IntegerValue(2),new IntegerValue(3));
        df1.addRow(new IntegerValue(0),new IntegerValue(0),new IntegerValue(0));
        df1.addRow(new IntegerValue(0),new IntegerValue(0),new IntegerValue(0));
        df1.addRow(new IntegerValue(4),new IntegerValue(5),new IntegerValue(6));

        SparseDataFrame sdf2 = new SparseDataFrame(df1, new IntegerValue(0));
        sdf2.printCOOValue();
        System.out.println(sdf2);


        DataFrame df = sdf.toDense();
        System.out.println(df);

        SparseDataFrame sdf4 = sdf.get(new String[]{"kol1","kol2"}, true);
        System.out.println(sdf4);


        System.out.println(" -- SparseDataFrame z wiersza [2] -- \n");
        System.out.println(sdf.iloc(2));
        System.out.println(" -- SparseDataFrame z wierszy [1,22] -- \n");
        System.out.println(sdf.iloc(1, 22));

        //SparseDataFrame read = new SparseDataFrame("X:\\maciejw\\java_course\\zaj1\\src\\sparseDataFrame\\sparse.csv", new String[] {"Double", "Double", "Double"}, true, "0.0");
        //System.out.println(read);

    }
}
