package pkg2;

import pkg1.*;
import java.util.Scanner;

public class C extends B {

    public C(String name, int number) {
        super(name, number);
    }

    @Override
    void changeName(){
        Scanner odczyt = new Scanner(System.in);
        String newName = (String) odczyt.nextLine();
        this.name = newName + " Zmienione w class C";
    }

}
