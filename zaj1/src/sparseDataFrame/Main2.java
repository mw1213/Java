package sparseDataFrame;

import dataframe.*;

public class Main2 {
    public static void main(String[] args){
        SparseDataFrame sdf = new SparseDataFrame(new String[]{"kol1", "kol2", "kol3", "kol4"}, new String[]{"int", "int", "int", "int"} , "0");
        sdf.addRow(1, 2, 3, 11);
        sdf.addRow(0, 0, 0, 11);
        sdf.addRow(0, 0, 0, 11);
        System.out.println(sdf.printSize());
        sdf.printCOOValue();
        sdf.addRow(4, 5, 6, 22);
        sdf.addRow(0, 0, 0, 22);
        System.out.println(sdf.printSize());
        sdf.printCOOValue();
        sdf.addRow(0, 0, 0, 22);
        sdf.addRow(7, 8, 9, 22);
        System.out.println(sdf.printSize());
        sdf.printCOOValue();
        System.out.println(sdf);

        DataFrame df1 = new DataFrame(new String[]{"kl1", "kl2", "kl3"}, new String[]{"int", "int", "int"});
        df1.addRow(1,2,3);
        df1.addRow(0,0,0);
        df1.addRow(0,0,0);
        df1.addRow(4,5,6);

        SparseDataFrame sdf2 = new SparseDataFrame(df1, "0");
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

    }
}
