package main;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import main.interfaces.*;
import main.interfaces.ContainerInfo.Type;
import main.interfaces.ItemInfo.ImportExportInfo;
import main.interfaces.ItemInfo.RetrievalDeliveryInfo;
import main.interfaces.ItemState;
import main.interfaces.LogInfo.StaffType;


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
			Properties properties = new Properties();
			properties.setProperty("user", root);
			properties.setProperty("password", pass);
			properties.setProperty("useSSL", "false");
			properties.setProperty("autoReconnect", "true");
			conn = DriverManager.getConnection("jdbc:postgresql://" + database, root, pass);
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
		statement.setString(1, logInfo.name());
		statement.setString(2, logInfo.password());
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
	
	
	private static final String getCompanyCountSQL = "SELECT count(*) FROM (SELECT DISTINCT company FROM staff WHERE company IS NOT NULL) tb";
	//SUSTC Department Manager User
	@Override
	public int getCompanyCount(LogInfo logInfo) {
		try {
			if (logInfo.type() != StaffType.SustcManager || !checkUser(logInfo)) {
				return -1;
			}
			
			PreparedStatement statement = this.conn.prepareStatement(getCompanyCountSQL);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				return rs.getInt(1);
			} else {
				return 0;
			}
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return -1;
	}
	
	private static final String getCityCountSQL = "SELECT count(*) from (\r\n"
			+ "    SELECT DISTINCT city FROM (\r\n"
			+ "        SELECT DISTINCT city FROM retrieval_information UNION DISTINCT\r\n"
			+ "        (SELECT DISTINCT city FROM delivery_information) UNION DISTINCT\r\n"
			+ "        (SELECT DISTINCT city FROM export_information) UNION DISTINCT\r\n"
			+ "        (SELECT DISTINCT city from import_information)\r\n"
			+ "        )t1)t0;";
	//SUSTC Department Manager User
	@Override
	public int getCityCount(LogInfo logInfo) {
		try {
			if (logInfo.type() != StaffType.SustcManager || !checkUser(logInfo)) {
				return -1;
			}
			
			PreparedStatement statement = this.conn.prepareStatement(getCityCountSQL);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				return rs.getInt(1);
			} else {
				return 0;
			}
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return -1;
	}
	
	private static final String getCourierCountSQL = "SELECT count(*) FROM staff WHERE type = 'Courier'";
	//SUSTC Department Manager User
	@Override
	public int getCourierCount(LogInfo logInfo) {
		try {
			if (logInfo.type() != StaffType.SustcManager || !checkUser(logInfo)) {
				return -1;
			}
			
			PreparedStatement statement = this.conn.prepareStatement(getCourierCountSQL);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				return rs.getInt(1);
			} else {
				return 0;
			}
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return -1;
	}
	
	private static final String getShipCountSQL = "SELECT count(*) FROM (SELECT DISTINCT ship_name FROM ship WHERE ship_name IS NOT NULl)t1;";
	//SUSTC Department Manager User
	@Override
	public int getShipCount(LogInfo logInfo) {
		try {
			if (logInfo.type() != StaffType.SustcManager || !checkUser(logInfo)) {
				return -1;
			}
			
			PreparedStatement statement = this.conn.prepareStatement(getShipCountSQL);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				return rs.getInt(1);
			} else {
				return 0;
			}
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return -1;
	}
	
	private static final Map<String, ItemState> stringStateMap = new HashMap<>();
	private static final Map<ItemState, String> stateStringMap = new HashMap<>();
	
	static {
		stringStateMap.put("Picking-up", ItemState.PickingUp);
		stringStateMap.put("To-Export Transporting", ItemState.ToExportTransporting);
		stringStateMap.put("Export Checking", ItemState.ExportChecking);
		stringStateMap.put("Export Check Fail", ItemState.ExportCheckFailed);
		stringStateMap.put("Packing to Container", ItemState.PackingToContainer);
		stringStateMap.put("Waiting for Shipping", ItemState.WaitingForShipping);
		stringStateMap.put("Shipping", ItemState.Shipping);
		stringStateMap.put("Unpacking from Container", ItemState.UnpackingFromContainer);
		stringStateMap.put("Import Checking", ItemState.ImportChecking);
		stringStateMap.put("Import Check Fail", ItemState.ImportCheckFailed);
		stringStateMap.put("From-Import Transporting", ItemState.FromImportTransporting);
		stringStateMap.put("Delivering", ItemState.Delivering);
		stringStateMap.put("Finish", ItemState.Finish);
		
		for (Map.Entry<String, ItemState> entry : stringStateMap.entrySet()) {
			stateStringMap.put(entry.getValue(), entry.getKey());
		}
		
	}
	
	private static ItemState getItemStateByDescription(String message) {
		return stringStateMap.get(message);
	}
	
	private static final String getItemSQL = "SELECT * FROM item WHERE name = ?";
	private static final String getImportSQL = "SELECT city, staff_name, tax FROM import_information WHERE item_name = ?";
	private static final String getExportSQL = "SELECT city, staff_name, tax FROM export_information WHERE item_name = ?";
	private static final String getRetrievalSQL = "SELECT city, staff_name FROM retrieval_information WHERE item_name = ?";
	private static final String getDeliverySQL = "SELECT city, staff_name FROM delivery_information WHERE item_name = ?";
	//SUSTC Department Manager User
	@Override
	public ItemInfo getItemInfo(LogInfo logInfo, String s) {
		try {
			if (logInfo.type() != StaffType.SustcManager || !checkUser(logInfo)) {
				return null;
			}
			PreparedStatement itemStatement = this.conn.prepareStatement(getItemSQL);
			itemStatement.setString(1, s);
			ResultSet itemRs = itemStatement.executeQuery();
			if (itemRs.next()) {
				PreparedStatement importStatement = this.conn.prepareStatement(getImportSQL);
				importStatement.setString(1, s);
				ResultSet importRs = importStatement.executeQuery();
				importRs.next();
				ImportExportInfo importInfo = new ImportExportInfo(importRs.getString(1), importRs.getString(2), importRs.getDouble(3));
				
				PreparedStatement exportStatement = this.conn.prepareStatement(getExportSQL);
				exportStatement.setString(1, s);
				ResultSet exportRs = exportStatement.executeQuery();
				exportRs.next();
				ImportExportInfo exportInfo = new ImportExportInfo(exportRs.getString(1), exportRs.getString(2), exportRs.getDouble(3));
				
				PreparedStatement retrievalStatement = this.conn.prepareStatement(getRetrievalSQL);
				retrievalStatement.setString(1, s);
				ResultSet retrievalRs = retrievalStatement.executeQuery();
				retrievalRs.next();
				RetrievalDeliveryInfo retrievalInfo = new RetrievalDeliveryInfo(retrievalRs.getString(1), retrievalRs.getString(2));
				
				PreparedStatement deliveryStatement = this.conn.prepareStatement(getDeliverySQL);
				deliveryStatement.setString(1, s);
				ResultSet deliveryRs = deliveryStatement.executeQuery();
				deliveryRs.next();
				RetrievalDeliveryInfo deliveryInfo = new RetrievalDeliveryInfo(deliveryRs.getString(1), deliveryRs.getString(2));

				ItemInfo itemInfo = new ItemInfo(itemRs.getString(1), itemRs.getString(2), itemRs.getDouble(3), getItemStateByDescription(itemRs.getString(4)), retrievalInfo, deliveryInfo, importInfo, exportInfo);
				return itemInfo;
			} else {
				return null;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return null;
	}
	
	private static final String getShipInfoSQL = "SELECT item_name, company FROM ship WHERE ship_name = ?";
	//SUSTC Department Manager User
	@Override
	public ShipInfo getShipInfo(LogInfo logInfo, String s) {
		try {
			if (logInfo.type() != StaffType.SustcManager || !checkUser(logInfo)) {
				return null;
			}
			PreparedStatement shipStatement = this.conn.prepareStatement(getShipInfoSQL);
			shipStatement.setString(1, s);
			ResultSet shipRs = shipStatement.executeQuery();
			boolean isSailing = false;
			String company = null;
			while (shipRs.next()) {
				if (company == null) {
					company = shipRs.getString(2);
				}
				PreparedStatement itemStatement = this.conn.prepareStatement(getItemSQL);
				itemStatement.setString(1, shipRs.getString(1));
				ResultSet itemRs = itemStatement.executeQuery();
				itemRs.next();
				isSailing |= getItemStateByDescription(itemRs.getString(4)) == ItemState.Shipping;
				if (isSailing) {
					break;
				}
			}
			if (company == null) {
				return null;
			}
			ShipInfo shipInfo = new ShipInfo(s, company, isSailing);
			return shipInfo;
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return null;
	}
	
	private static final Map<String, Type> stringContainerTypeMap = new HashMap<>();
	private static final Map<Type, String> containerTypeStringMap = new HashMap<>();
	static {
		stringContainerTypeMap.put("Dry Container", Type.Dry);
		stringContainerTypeMap.put("Flat Rack Container", Type.FlatRack);
		stringContainerTypeMap.put("Open Top Container", Type.OpenTop);
		stringContainerTypeMap.put("ISO Tank Container", Type.ISOTank);
		stringContainerTypeMap.put("Reefer Container", Type.Reefer);
		
		for (Map.Entry<String, Type> entry : stringContainerTypeMap.entrySet()) {
			containerTypeStringMap.put(entry.getValue(), entry.getKey());
		}
	}
	
	private static Type getContainerTypeByDescription(String description) {
		return stringContainerTypeMap.get(description);
	}
	
	private static final String getContainerInfoSQL = "SELECT item_name, type FROM container WHERE code = ?";
	//SUSTC Department Manager User
	@Override
	public ContainerInfo getContainerInfo(LogInfo logInfo, String s) {
		try {
			if (logInfo.type() != StaffType.SustcManager || !checkUser(logInfo)) {
				return null;
			}
			PreparedStatement containerStatement = this.conn.prepareStatement(getContainerInfoSQL);
			containerStatement.setString(1, s);
			ResultSet containerRs = containerStatement.executeQuery();
			boolean isUsing = false;
			String type = null;
			while (containerRs.next()) {
				if (type == null) {
					type = containerRs.getString(2);
				}
				PreparedStatement itemStatement = this.conn.prepareStatement(getItemSQL);
				itemStatement.setString(1, containerRs.getString(1));
				ResultSet itemRs = itemStatement.executeQuery();
				itemRs.next();
				ItemState state = getItemStateByDescription(itemRs.getString(4));
				isUsing |= (state == ItemState.WaitingForShipping) || (state == ItemState.Shipping) || (state == ItemState.UnpackingFromContainer) || (state == ItemState.PackingToContainer);
				if (isUsing) {
					break;
				}
			}
			if (type == null) {
				return null;
			}
			ContainerInfo containerInfo = new ContainerInfo(getContainerTypeByDescription(type), s, isUsing);
			return containerInfo;
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return null;
	}
	
	private static final Map<String, StaffType> stringStaffTypeMap = new HashMap<>();
	private static final Map<StaffType, String> staffTypeStringMap = new HashMap<>();
	
	static {
		stringStaffTypeMap.put("Courier", StaffType.Courier);
		stringStaffTypeMap.put("SUSTC Department Manager", StaffType.SustcManager);
		stringStaffTypeMap.put("Company Manager", StaffType.CompanyManager);
		stringStaffTypeMap.put("Seaport Officer", StaffType.SeaportOfficer);
		
		for (Map.Entry<String, StaffType> entry : stringStaffTypeMap.entrySet()) {
			staffTypeStringMap.put(entry.getValue(), entry.getKey());
		}
		
	}
	
	private static StaffType getStaffTypeByDescription(String description) {
		return stringStaffTypeMap.get(description);
	}
	
	private static final String getStaffInfoSQL = "SELECT * FROM staff WHERE name = ?";
	//SUSTC Department Manager User
	@Override
	public StaffInfo getStaffInfo(LogInfo logInfo, String s) {
		try {
			if (logInfo.type() != StaffType.SustcManager || !checkUser(logInfo)) {
				return null;
			}
			PreparedStatement statement = this.conn.prepareStatement(getStaffInfoSQL);
			statement.setString(1, s);
			ResultSet rs = statement.executeQuery();
			if (!rs.next()) {
				return null;
			}
			int birthYear = rs.getInt(7);
			int currentYear = Calendar.getInstance().get(Calendar.YEAR);
			return new StaffInfo(new LogInfo(s, getStaffTypeByDescription(rs.getString(3)), rs.getString(2)), rs.getString(8), rs.getString(4), rs.getBoolean(5), currentYear - birthYear, rs.getString(6));
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return null;
	}

}
