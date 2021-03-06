package sparseDataFrame;

import dataframe.*;
import myExceptions.*;
import value.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class Main2 {
    public static void main(String[] args) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
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
        try {
            df1.addRow(new IntegerValue(1),new IntegerValue(2),new IntegerValue(3));
            df1.addRow(new IntegerValue(0),new IntegerValue(0),new IntegerValue(0));
            df1.addRow(new IntegerValue(0),new IntegerValue(0),new IntegerValue(0));
            df1.addRow(new IntegerValue(4),new IntegerValue(5),new IntegerValue(6));
        } catch (WrongTypeInColumnException e) {
            e.printMessage();
        }


        SparseDataFrame sdf2 = new SparseDataFrame(df1, new IntegerValue(0));
        sdf2.printCOOValue();
        System.out.println(sdf2);


        DataFrame df = null;
        try {
            df = sdf.toDense();
        } catch (AddingWrongClassesException e) {
            e.printMessage();
        }
        System.out.println(df);

        SparseDataFrame sdf4 = sdf.get(new String[]{"kol1","kol2"}, true);
        System.out.println(sdf4);


        System.out.println(" -- SparseDataFrame z wiersza [2] -- \n");
        System.out.println(sdf.iloc(2));
        System.out.println(" -- SparseDataFrame z wierszy [1,22] -- \n");
        System.out.println(sdf.iloc(1, 22));

        //SparseDataFrame read = new SparseDataFrame("/home/maciej/IdeaProjects/Java/zaj1/src/sparseDataFrame/sparse.csv", new Class[]{DoubleValue.class, DoubleValue.class, DoubleValue.class}, true, new DoubleValue().create("0.0"));
        //SparseDataFrame read = new SparseDataFrame("X:\\maciejw\\java_course\\zaj1\\src\\sparseDataFrame\\sparse.csv", new Class[]{DoubleValue.class, DoubleValue.class, DoubleValue.class}, true, new DoubleValue().create("0.0"));
        SparseDataFrame read = new SparseDataFrame("C:\\Users\\Maciej Wilk\\IdeaProjects\\java_course\\Java\\zaj1\\src\\SparseDataFrame\\sparse.csv", new Class[]{DoubleValue.class, DoubleValue.class, DoubleValue.class}, true, new DoubleValue().create("0.0"));
        System.out.println(read);

        System.out.println(sdf.get("kol1"));

    }
}
