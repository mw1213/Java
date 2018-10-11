package pkg1;

import java.util.Scanner;

public class A {
    String name;
    protected int number;

    public A(String name, int number) {
        this.name = name;
        this.number = number;
    }
    public void callDecrement(){
        decrement();
        System.out.println("Nowa wartość pola number: "+ number);
    }

    public void callChangeName(){
        changeName();
        System.out.println("Nowa nazwa: "+ name);
    }

    public void callIncrement(){
        increment();
        System.out.println("Nowa wartość pola number: "+ number);
    }

    private void increment(){
        this.number++;
    }

    void decrement(){
        this.number--;
    }

    void changeName(){
        Scanner odczyt = new Scanner(System.in);
        String newName = odczyt.nextLine();
        this.name = newName;
    }


}
