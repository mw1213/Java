package myPackage;

public class LiczbyPierwsze {
    private int liczba;

    public LiczbyPierwsze(int _liczba){
        liczba=_liczba;
    }

    public LiczbyPierwsze(){
        liczba=0;
    }

    public void check(){
        int i, j, doKad;
        int tablica[] = new int[liczba+1];
        doKad = (int) Math.floor(Math.sqrt(liczba));
        for (i=1; i<=liczba; i++) tablica[i]=i;

        for (i=2; i<=doKad; i++){
            if (tablica[i] != 0){
                j=i+i;
                while (j<=liczba) {
                    tablica[j]=0;
                    j += i;
                }
            }
        }
        System.out.println("Liczby pierwsze z zakresu od 1 do " + liczba);
        for (i=2; i<=liczba; i++) if (tablica[i]!=0) System.out.print(i + ", ");
    }


}
