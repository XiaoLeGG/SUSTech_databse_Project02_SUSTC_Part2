import java.sql.*;
public class Main {
    private static String url = "jdbc:postgresql://localhost:5432/postgres";
        private static String user = "postgres";
        private static String pass = "ljcfyh_123@99";
        private static Connection con;
        public static void main(String[] args) {
            DatabaseManager databaseManager = new DatabaseManager(url, user, pass);
            databaseManager.$import("data/records.csv","data/staffs.csv");
    }
}
