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

        DataFrame df = sdf.toDense();
        System.out.println(df);

    }
}
