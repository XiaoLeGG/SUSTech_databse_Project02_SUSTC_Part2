package main;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Scanner;

import cs307.project2.interfaces.ContainerInfo;
import cs307.project2.interfaces.IDatabaseManipulation;
import cs307.project2.interfaces.ItemInfo;
import cs307.project2.interfaces.ItemState;
import cs307.project2.interfaces.LogInfo;
import cs307.project2.interfaces.ShipInfo;
import cs307.project2.interfaces.StaffInfo;
import cs307.project2.interfaces.LogInfo.StaffType;

public class DBManipulation implements IDatabaseManipulation {
	
	private String database, root, pass;
	private Connection conn;
	
	static {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public DBManipulation(String database, String root, String pass) {
		this.database = database;
		this.root = root;
		this.pass = pass;
		try {
			conn = DriverManager.getConnection(database, root, pass);
			Statement sta = this.conn.createStatement();
			sta.executeUpdate("create table if not exists staff (" 
					+ "    name varchar not null,"
					+ "    password varchar not null," 
					+ "    type varchar not null," 
					+ "    city varchar,"
					+ "    gender boolean not null," 
					+ "    phone_number varchar not null,"
					+ "    birth_year integer not null," 
					+ "    company varchar," 
					+ "    primary key (name)" 
					+ ");"
					+ "create table if not exists export_information (" 
					+ "    item_name varchar not null,"
					+ "    city varchar not null," 
					+ "    tax numeric(20, 7) not null,"
					+ "    staff_name varchar references staff(name)," 
					+ "    primary key (item_name)" 
					+ ");"
					+ "create table if not exists import_information(" 
					+ "    item_name varchar not null,"
					+ "    city varchar not null," 
					+ "    tax numeric(20, 7) not null,"
					+ "    staff_name varchar references staff(name)," 
					+ "    primary key (item_name)" 
					+ ");"
					+ "create table if not exists ship(" 
					+ "    item_name varchar not null," 
					+ "    ship_name varchar,"
					+ "    company varchar not null," 
					+ "    primary key (item_name)" 
					+ ");"
					+ "create table if not exists container(" 
					+ "    item_name varchar not null," 
					+ "    code varchar,"
					+ "    type varchar," 
					+ "    primary key (item_name)" 
					+ ");"
					+ "create table if not exists retrieval_information(" 
					+ "    item_name varchar not null,"
					+ "    city varchar not null," 
					+ "    staff_name varchar not null references staff(name),"
					+ "    primary key (item_name)" 
					+ ");" 
					+ "create table if not exists delivery_information("
					+ "    item_name varchar not null," 
					+ "    city varchar not null,"
					+ "    staff_name varchar references staff(name)," 
					+ "    primary key (item_name)" + ");"
					+ "create table if not exists item(" 
					+ "    name varchar not null," 
					+ "    type varchar not null,"
					+ "    price numeric(20, 7) not null," 
					+ "    state varchar not null," 
					+ "    primary key (name)"
					+ ");"
					+ "alter table delivery_information add constraint foreignKey_deliveryInformation_itemName foreign key (item_name) references item(name);"
					+ "alter table retrieval_information add constraint foreignKey_retrievalInformation_itemName foreign key (item_name) references item(name);"
					+ "alter table export_information add constraint foreignKey_exportInformation_itemName foreign key (item_name) references item(name);"
					+ "alter table import_information add constraint foreignKey_importInformation_itemName foreign key (item_name) references item(name);"
					+ "alter table ship add constraint foreignKey_ship_itemName foreign key (item_name) references item(name);"
					+ "alter table container add constraint foreignKey_container_itemName foreign key (item_name) references item(name);");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private static final String containerSQL = "INSERT INTO container(item_name, code, type) VALUES(?, ?, ?)";
	private static final String deliveryInformationSQL = "INSERT INTO delivery_information(item_name, city, staff_name) VALUES(?, ?, ?)";
	private static final String retrievalInformationSQL = "INSERT INTO retrieval_information(item_name, city, staff_name) VALUES(?, ?, ?)";
	private static final String exportInformationSQL = "INSERT INTO export_information(item_name, city, tax, staff_name) VALUES(?, ?, ?, ?)";
	private static final String importInformationSQL = "INSERT INTO import_information(item_name, city, tax, staff_name) VALUES(?, ?, ?, ?)";
	private static final String itemSQL = "INSERT INTO item(name, type, price, state) VALUES(?, ?, ?, ?)";
	private static final String shipSQL = "INSERT INTO ship(item_name, ship_name, company) VALUES(?, ?, ?)";
	private static final String staffSQL = "INSERT INTO staff(name, password, type, city, gender, phone_number, birth_year, company) VALUES(?, ?, ?, ?, ?, ?, ?, ?)";

	@Override
	public void $import(String recordsCSV, String staffsCSV) {
		long start = System.currentTimeMillis();
		try {
			this.conn.prepareStatement("ALTER TABLE delivery_information DISABLE TRIGGER ALL").execute();
			this.conn.prepareStatement("ALTER TABLE retrieval_information DISABLE TRIGGER ALL").execute();
			this.conn.prepareStatement("ALTER TABLE export_information DISABLE TRIGGER ALL").execute();
			this.conn.prepareStatement("ALTER TABLE import_information DISABLE TRIGGER ALL").execute();
			this.conn.prepareStatement("ALTER TABLE ship DISABLE TRIGGER ALL").execute();
			this.conn.prepareStatement("ALTER TABLE container DISABLE TRIGGER ALL").execute();
			Scanner scanner = new Scanner(staffsCSV);
			Scanner valueScanner = null;
			this.conn.setAutoCommit(false);
			int index = 0;
			Calendar cal = Calendar.getInstance();
			int currentYear = cal.get(Calendar.YEAR);
			int birthYear = 0;
			boolean gender = false;
			PreparedStatement statement = conn.prepareStatement(staffSQL);
			scanner.nextLine();
			while (scanner.hasNextLine()) {
				valueScanner = new Scanner(scanner.nextLine());
				valueScanner.useDelimiter(",");
				String password = null;
				String type = null;
				String city = null;
				String phone_number = null;
				String company = null;
				String name = null;
				while (valueScanner.hasNext()) {
					String data = valueScanner.next();
					if (data.equals("")) {
						data = null;
					}
					if (index == 0) {
						name = data;
					}
					if (index == 1) {
						type = data;
					}
					if (index == 2) {
						company = data;						
					}
					if (index == 3) {
						city = data;
					}
					if (index == 4) {
						if (data.equals("female")) {
							gender = true;
						} else {
							gender = false;
						}
					}
					if (index == 5) {
						birthYear = currentYear - Integer.parseInt(data);
					}
					if (index == 6) {
						phone_number = data;
					}
					if (index == 7) {
						password = data;
					}
					index++;
				}
				index = 0;
				statement.setString(1, name);
				statement.setString(2, password);
				statement.setString(3, type);
				statement.setString(4, city);
				statement.setBoolean(5, gender);
				statement.setString(6, phone_number);
				statement.setInt(7, birthYear);
				statement.setString(8, company);
				statement.addBatch();
			}
			statement.executeBatch();
			scanner.close();
			scanner = new Scanner(recordsCSV);
			valueScanner = null;
			
			PreparedStatement containerStatement = this.conn.prepareStatement(containerSQL);
			PreparedStatement deliveryInformationStatement = this.conn.prepareStatement(deliveryInformationSQL);
			PreparedStatement retrievalInformationStatement = this.conn.prepareStatement(retrievalInformationSQL);
			PreparedStatement exportInformationStatement = this.conn.prepareStatement(exportInformationSQL);
			PreparedStatement importInformationStatement = this.conn.prepareStatement(importInformationSQL);
			PreparedStatement itemStatement = this.conn.prepareStatement(itemSQL);
			PreparedStatement shipStatement = this.conn.prepareStatement(shipSQL);
			scanner.nextLine();
			int cnt = 0;
			String data = null;
			while (scanner.hasNextLine()) {
				
				String itemName = null;
				String itemClass = null;
				String retrievalCity = null;
				String retrievalCourier = null;
				String deliveryCity = null;
				String deliveryCourier = null;
				String exportCity = null;
				String importCity = null;
				String exportOfficer = null;
				String importOfficer = null;
				String containerCode = null;
				String containerType = null;
				String shipName = null;
				String companyName = null;
				String itemState = null;
				
				double itemPrice = 0;
				double exportTax = 0;
				double importTax = 0;
				
				valueScanner = new Scanner(scanner.nextLine());
				valueScanner.useDelimiter(",");
				
				while (valueScanner.hasNext()) {
					data = valueScanner.next();
					if (data.equals(""))
						data = null;
					if (index == 0) {
						itemName = data;
					}
					if (index == 1) {
						itemClass = data;
					}
					if (index == 2) {
						itemPrice = Double.parseDouble(data);
					}
					if (index == 3) {
						retrievalCity = data;
					}
					if (index == 4) {
						retrievalCourier = data;
					}
					if (index == 5) {
						deliveryCity = data;
					}
					if (index == 6) {
						deliveryCourier = data;
					}
					if (index == 7) {
						exportCity = data;
					}
					if (index == 8) {
						importCity = data;
					}
					if (index == 9) {
						exportTax = Double.parseDouble(data);
					}
					if (index == 10) {
						importTax = Double.parseDouble(data);
					}
					if (index == 11) {
						exportOfficer = data;
					}
					if (index == 12) {
						importOfficer = data;
					}
					if (index == 13) {
						containerCode = data;
					}
					if (index == 14) {
						containerType = data;
					}
					if (index == 15) {
						shipName = data;
					}
					if (index == 16) {
						companyName = data;
					}
					if (index == 17) {
						itemState = data;
					}
					index++;
				}
				
				index = 0;
				cnt++;
				
				containerStatement.setString(1, itemName);
				containerStatement.setString(2, containerCode);
				containerStatement.setString(3, containerType);
				containerStatement.addBatch();
				
				deliveryInformationStatement.setString(1, itemName);
				deliveryInformationStatement.setString(2, deliveryCity);
				deliveryInformationStatement.setString(3, deliveryCourier);
				deliveryInformationStatement.addBatch();
				
				exportInformationStatement.setString(1, itemName);
				exportInformationStatement.setString(2, exportCity);
				exportInformationStatement.setDouble(3, exportTax);
				exportInformationStatement.setString(4, exportOfficer);
				exportInformationStatement.addBatch();
				
				importInformationStatement.setString(1, itemName);
				importInformationStatement.setString(2, importCity);
				importInformationStatement.setDouble(3, importTax);
				importInformationStatement.setString(4, importOfficer);
				importInformationStatement.addBatch();
				
				itemStatement.setString(1, itemName);
				itemStatement.setString(2, itemClass);
				itemStatement.setDouble(3, itemPrice);
				itemStatement.setString(4, itemState);
				itemStatement.addBatch();
				
				retrievalInformationStatement.setString(1, itemName);
				retrievalInformationStatement.setString(2, retrievalCity);
				retrievalInformationStatement.setString(3, retrievalCourier);
				retrievalInformationStatement.addBatch();
				
				shipStatement.setString(1, itemName);
				shipStatement.setString(2, shipName);
				shipStatement.setString(3, companyName);
				shipStatement.addBatch();
				if (cnt % 3000 == 0) {
					containerStatement.executeBatch();
					deliveryInformationStatement.executeBatch();
					exportInformationStatement.executeBatch();
					importInformationStatement.executeBatch();
					itemStatement.executeBatch();
					retrievalInformationStatement.executeBatch();
					shipStatement.executeBatch();
				}
			}
			containerStatement.executeBatch();
			deliveryInformationStatement.executeBatch();
			exportInformationStatement.executeBatch();
			importInformationStatement.executeBatch();
			itemStatement.executeBatch();
			retrievalInformationStatement.executeBatch();
			shipStatement.executeBatch();
			
			this.conn.prepareStatement("ALTER TABLE delivery_information ENABLE TRIGGER ALL").execute();
			this.conn.prepareStatement("ALTER TABLE retrieval_information ENABLE TRIGGER ALL").execute();
			this.conn.prepareStatement("ALTER TABLE export_information ENABLE TRIGGER ALL").execute();
			this.conn.prepareStatement("ALTER TABLE import_information ENABLE TRIGGER ALL").execute();
			this.conn.prepareStatement("ALTER TABLE ship ENABLE TRIGGER ALL").execute();
			this.conn.prepareStatement("ALTER TABLE container ENABLE TRIGGER ALL").execute();
			this.conn.commit();
			this.conn.setAutoCommit(true);
			scanner.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		System.out.println("Total Time = " + (end - start) + " ms");
	}
	
	//Need Type Check?
	private static final String checkUserSQL = "SELECT * FROM staff where name = ? AND password = ? ";
	
	
	private boolean checkUser(LogInfo logInfo) throws SQLException {
		PreparedStatement statement = this.conn.prepareStatement(checkUserSQL);
		ResultSet rs = statement.executeQuery();
		return rs.next();
	}
		
	
	
	private static final String getImportTaxRateSQL = "";
	//Company Manager User
	@Override
	public double getImportTaxRate(LogInfo logInfo, String s, String s1) {
		try {
			if (logInfo.type() != StaffType.CompanyManager || !checkUser(logInfo)) {
				return -1;
			}
			
			
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		
		return 0;
	}
	
	//Company Manager User
	@Override
	public double getExportTaxRate(LogInfo logInfo, String s, String s1) {
		return 0;
	}
	
	//Company Manager User
	@Override
	public boolean loadItemToContainer(LogInfo logInfo, String s, String s1) {
		return false;
	}
	
	//Company Manager User
	@Override
	public boolean loadContainerToShip(LogInfo logInfo, String s, String s1) {
		return false;
	}
	
	//Company Manager User
	@Override
	public boolean shipStartSailing(LogInfo logInfo, String s) {
		return false;
	}
	
	//Company Manager User
	@Override
	public boolean unloadItem(LogInfo logInfo, String s) {
		return false;
	}
	
	//Company Manager User
	@Override
	public boolean itemWaitForChecking(LogInfo logInfo, String s) {
		return false;
	}
	
	//Courier User
	@Override
	public boolean newItem(LogInfo logInfo, ItemInfo itemInfo) {
		return false;
	}
	
	//Courier User
	@Override
	public boolean setItemState(LogInfo logInfo, String s, ItemState itemState) {
		return false;
	}
	
	//Seaport Officer User
	@Override
	public String[] getAllItemsAtPort(LogInfo logInfo) {
		return new String[0];
	}
	
	//Seaport Officer User
	@Override
	public boolean setItemCheckState(LogInfo logInfo, String s, boolean b) {
		return false;
	}
	
	
	private static final String getCompanyCountSQL = "select count(*) from ";
	//SUSTC Department Manager User
	@Override
	public int getCompanyCount(LogInfo logInfo) {
		try {
			if (logInfo.type() != StaffType.SustcManager || !checkUser(logInfo)) {
				return -1;
			}
			
			
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return 0;
	}
	
	//SUSTC Department Manager User
	@Override
	public int getCityCount(LogInfo logInfo) {
		return 0;
	}
	
	//SUSTC Department Manager User
	@Override
	public int getCourierCount(LogInfo logInfo) {
		return 0;
	}
	
	//SUSTC Department Manager User
	@Override
	public int getShipCount(LogInfo logInfo) {
		return 0;
	}
	
	//SUSTC Department Manager User
	@Override
	public ItemInfo getItemInfo(LogInfo logInfo, String s) {
		return null;
	}
	
	//SUSTC Department Manager User
	@Override
	public ShipInfo getShipInfo(LogInfo logInfo, String s) {
		return null;
	}
	
	//SUSTC Department Manager User
	@Override
	public ContainerInfo getContainerInfo(LogInfo logInfo, String s) {
		return null;
	}
	
	//SUSTC Department Manager User
	@Override
	public StaffInfo getStaffInfo(LogInfo logInfo, String s) {
		return null;
	}

}
