package myPackage;

import java.io.*;

public class LoginProgram {
    public static void main(String[] argv){
        Login log = new Login ("ala", "makota");
            InputStreamReader rd = new InputStreamReader(System.in);
            BufferedReader bfr = new BufferedReader(rd);
            String login = null;
            String haslo = null;
            // TODO: prosba o wpisanie hasła i loginu
            System.out.print("Wpisz login: ");
            try {
                login = bfr.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Wpisales: "+login);

            System.out.print("Wpisz hasło: ");
            try {
                haslo = bfr.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Wpisales: "+haslo);


        /*TODO: sprawdzenie czy podane hasło i login zgadzaja sie z tymi
         przechowywanymi w obiekcie log Jeśli tak, to ma zostać
         wyswietlone "OK", jesli nie - prosze wyswietlić informacje o błedzie
         */
            if (log.check(login, haslo)){
                System.out.println("OK");
            }
            else System.out.println("Podano złe dane!");

    }
}