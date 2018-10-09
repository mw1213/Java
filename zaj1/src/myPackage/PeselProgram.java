package myPackage;
import java.util.Scanner;


public class PeselProgram {
    public static void main (String[] argv){
        Scanner odczyt = new Scanner(System.in);
        Pesel pesel = new Pesel(odczyt.nextLine());
        pesel.check();
    }
}
