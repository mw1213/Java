package myPackage;

public class Pesel {
    private String pesel;

    public Pesel(String _pesel){
        pesel=_pesel;
    }

    public boolean check(){
        int length = pesel.length();
        if (length !=11){
            System.out.println("Niepoprawny PESEL!");
            return false;
        }
        char checkChar = pesel.charAt(0);


        for (int i=0; i<length; i++){
            checkChar = pesel.charAt(i);
            if (checkChar<'0'|| checkChar>'9'){
                System.out.println("Niepoprawny PESEL!");
                return false;
            }
        }

        checkChar = pesel.charAt(2);
        if (checkChar>'3' || checkChar<'0'){
            System.out.println("Niepoprawny PESEL!");
            return false;
        }
        if (checkChar=='3' || checkChar=='1'){
            char newCheckChar = pesel.charAt(3);
            if (newCheckChar>'2' || newCheckChar<'0'){
                System.out.println("Niepoprawny PESEL!");
                return false;
            }
        }


        checkChar = pesel.charAt(4);
        if (checkChar>'3' || checkChar<'0'){
            System.out.println("Niepoprawny PESEL!");
            return false;
        }
        if (checkChar=='3' || checkChar=='1'){
            char newCheckChar = pesel.charAt(3);
            if (newCheckChar>'1' || newCheckChar<'0'){
                System.out.println("Niepoprawny PESEL!");
                return false;
            }
        }


        System.out.println("Poprawny PESEL!");
        return true;

    }
}
