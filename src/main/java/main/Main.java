package main;

public class Main {
	private static String url = "jdbc:postgresql://localhost:5432/cslab1";
	private static String user = "test";
	private static String pass = "123456";
	
	private static ThrowableHandler th;

	public static void main(String[] args) {
		DBManipulation databaseManager = new DBManipulation(url, user, pass);
		databaseManager.$import("data/records.csv", "data/staffs.csv");
		th = new ThrowableHandler();
	}
	
	protected static ThrowableHandler getThrowableHandler() {
		return th;
	}
	
}
