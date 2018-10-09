package myPackage;

import javaIn.*;

public class LiczbyPierwszeProgram {
    public static void main (String[] argv){
        System.out.println("Podaj liczbę:");
        LiczbyPierwsze liczba = new LiczbyPierwsze(JIn.getInt());
        liczba.check();



    }

}
