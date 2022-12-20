package main;

public class Main {
	private static String url = "localhost:5432/postgres";

	private static String user = "postgres";
	private static String pass = "ljcfyh_123@99";
	
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
