package myExceptions;

public class DifferentSizesOfColumnException extends Exception {
    private String[] colNames = new String[2];
    private int[] colSizes= new int[2];

    public DifferentSizesOfColumnException(String name1, String name2, int size1, int size2){
        colNames[0]=name1;
        colNames[1]=name2;
        colSizes[0]=size1;
        colSizes[1]=size2;
    }

    public String[] getColNames() {
        return colNames;
    }

    public int[] getColSizes() {
        return colSizes;
    }

    public void printMessage(){
        System.out.println("Wrong sizes of columns: \n Column \""+colNames[0]
                +"\" has: "+colSizes[0]+" elements.\nColumn \""+colNames[1]+"\" has: "+colSizes[1]+" elements.");
    }
}
