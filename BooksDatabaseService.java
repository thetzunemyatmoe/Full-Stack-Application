/*
 * BooksDatabaseService.java
 *
 * The service threads for the books database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: 2476895
 *
 */

import java.io.*;
//import java.io.OutputStreamWriter;

import java.net.ServerSocket;
import java.net.Socket;

import java.util.Arrays;
import java.util.StringTokenizer;

import java.sql.*;
import javax.sql.rowset.*;
//Direct import of the classes CachedRowSet and CachedRowSetImpl will fail becuase
//these clasess are not exported by the module. Instead, one needs to impor
//javax.sql.rowset.* as above.


public class BooksDatabaseService extends Thread {

    private Socket serviceSocket = null;
    private String[] requestStr = new String[2]; //One slot for author's name and one for library's name.
    private ResultSet outcome = null;

    //JDBC connection
    private String USERNAME = Credentials.USERNAME;
    private String PASSWORD = Credentials.PASSWORD;
    private String URL = Credentials.URL;


    //Class constructor
    public BooksDatabaseService(Socket aSocket) {
        this.serviceSocket = aSocket;
    }


    //Retrieve the request from the socket
    public String[] retrieveRequest() {
        this.requestStr[0] = ""; //For author
        this.requestStr[1] = ""; //For library
        String tmp = "";
        try {
            InputStreamReader socketReader = new InputStreamReader(serviceSocket.getInputStream());
            StringBuffer stringBuffer = new StringBuffer();
            char x;
            while (true) {
                x = (char) socketReader.read();
                if (x == '#') {
                    break;
                }
                stringBuffer.append(x);
            }
            tmp = stringBuffer.toString();
            String[] str_arr = tmp.split(";");

            this.requestStr[0] = str_arr[0];
            this.requestStr[1] = str_arr[1];
        } catch (IOException e) {
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        return this.requestStr;
    }

    //Parse the request command and execute the query
    public boolean attendRequest() {
        boolean flagRequestAttended = true;
        this.outcome = null;
        String familyname = this.requestStr[0];
        String city = this.requestStr[1];
        String sql = "SELECT b.title, b.publisher, b.genre, b.rrp, COUNT(bc.copyid) AS numcopies FROM author a JOIN book b ON a.authorid = b.authorid JOIN bookcopy bc ON b.bookid = bc.bookid JOIN library l ON bc.libraryid = l.libraryid WHERE a.familyname = '" + familyname + "'AND l.city = '" + city + "' GROUP BY b.title, b.publisher, b.genre, b.rrp HAVING COUNT(bc.copyid) > 0 ORDER BY b.title";

        try {
            Class.forName("org.postgresql.Driver");
            Connection con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            Statement stmt = con.createStatement();
            outcome = stmt.executeQuery(sql);
            RowSetFactory aFactory = RowSetProvider.newFactory();
            CachedRowSet crs = aFactory.createCachedRowSet();
            crs.populate(outcome);
            crs.beforeFirst();
            con.close();
            stmt.close();
            outcome.close();
            outcome = crs;
        } catch (Exception e) {
            System.out.println(e);
        }
        return flagRequestAttended;
    }


    //Wrap and return service outcome
    public void returnServiceOutcome() {
        try {
            ObjectOutputStream outcomeStreamWriter = new ObjectOutputStream(serviceSocket.getOutputStream());
            outcomeStreamWriter.writeObject(outcome);
            outcomeStreamWriter.flush();
            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);
            serviceSocket.close();
        } catch (IOException e) {
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
    }


    //The service thread run() method
    public void run() {
        try {
            System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
                    + "author->" + this.requestStr[0] + "; library->" + this.requestStr[1]);
            //Attend the request
            boolean tmp = this.attendRequest();
            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        } catch (Exception e) {
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}
