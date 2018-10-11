import pkg1.*;
import pkg2.*;

public class Main {
    public static void main (String[] args){
        A clA = new A("JestemA", 13);
        B clB = new B("JestemB", 23);
//        C clC = new B("JestemC", 34);

        clA.callDecrement();
        clB.callDecrement();
//        clC.callDecrement();

        clA.callIncrement();
        clB.callIncrement();
    }

}
