package dataFrameDB;

import dataframe.*;
import value.Value;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class DataFrameDB extends DataFrame {
    private Connection conn = null;
    private Statement stmt = null;
    private ResultSet rs = null;
    private PreparedStatement pstmt =null;

    private List<Class<? extends Value>> types;

    public void connect() {
        for (int i=0; i<3; i++){
            try {
                Class.forName("com.mysql.jdbc.Driver").newInstance();
                conn =
                        DriverManager.getConnection("jdbc:mysql://mysql.agh.edu.pl/maciejw",
                                "maciejw", "dEgqnqsG8ripMcK3");
                if (conn != null) {
                    break;
                }
            } catch (SQLException ex){

                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
            } catch (Exception e) {

                e.printStackTrace();
            }
        }
        if (conn != null){
            System.out.println("Connected succesfuly!");
        }
        else {
            System.out.println("Couldn't connect to database :(");
        }

    }

    public void createTable(DataFrame input, String name) {
        this.types = new ArrayList<Class<? extends Value>>(input.columns.size());
        for (int i=0; i<input.columns.size(); i++){
            types.add(input.columns.get(i).getType());
        }
        try {
            connect();
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            pstmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS "+ name +" ("
                    + "priKey INT NOT NULL AUTO_INCREMENT, "
                    + input.columns.get(0).getName()+" VARCHAR(64), PRIMARY KEY (priKey))");
            pstmt.execute();
            try {


                for (int i = 1; i < input.columns.size(); i++) {
                    pstmt = conn.prepareStatement("ALTER TABLE " + name + " ADD " + input.columns.get(i).getName() + " VARCHAR(64)");
                    pstmt.execute();
                }
            } catch (IndexOutOfBoundsException e){

            } catch (SQLException e){
                System.out.println("SQLException: "+ e.getMessage());
            }

        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        } finally {
            // zwalniamy zasoby, które nie będą potrzebne
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) {
                } // ignore
                rs = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                } // ignore

                stmt = null;
            }

            if (pstmt != null){
                try {
                    pstmt.close();
                } catch (SQLException e) {

                }
                pstmt =null;
            }
        }

    }

    public DataFrameDB(DataFrame dataFrame, String name){
        createTable(dataFrame, name);
        for (int a = 0; a < dataFrame.size(); a++) {
            try {
                pstmt = conn.prepareStatement("INSERT INTO " + name + " (" + dataFrame.columns.get(0).getName() + ") " +
                        "VALUE ('" + dataFrame.columns.get(0).elAtIndex(a).toString() + "')");
                pstmt.execute();
            } catch (SQLException ex) {
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
                ex.printStackTrace();
            }
        }
        for (int j=1; j<dataFrame.columns.size(); j++) {
            for (int i = 0; i < dataFrame.size(); i++) {
                try {
                    pstmt = conn.prepareStatement("UPDATE "+name+" SET "+dataFrame.columns.get(j).getName() +
                            " = '"+dataFrame.columns.get(j).elAtIndex(i).toString()+"' WHERE "+dataFrame.columns.get(0).getName()+" = '"+dataFrame.columns.get(0).elAtIndex(i)+"' AND priKey = "+(i+1));
                    pstmt.execute();
                } catch (SQLException ex) {
                    System.out.println("SQLException: " + ex.getMessage());
                    System.out.println("SQLState: " + ex.getSQLState());
                    System.out.println("VendorError: " + ex.getErrorCode());
                }
            }
        }
    }

}
