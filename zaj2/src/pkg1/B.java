package pkg1;

import java.util.Scanner;

public class B extends A {
    public B(String name, int number) {
        super(name, number);
    }

    protected void decrement(){
        int newNumber = this.number;
        newNumber = newNumber -2;
        this.number = newNumber;
    }

    private void increment(){
        int newNumber = this.number;
        newNumber = newNumber + 2;
        this.number = newNumber;
    }

    @Override
    void changeName(){
        Scanner odczyt = new Scanner(System.in);
        String newName = (String) odczyt.nextLine();
        this.name = newName + "Zmienione w class B";
    }
}
