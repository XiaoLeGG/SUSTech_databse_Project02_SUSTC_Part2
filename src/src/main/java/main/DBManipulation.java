package main;


import java.sql.*;
import java.util.*;

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


	//Company Manager User
	private static final String getImportTaxRateSQL = "with\n" +
			"    tmp1 as (select name, price from item where type = ?),\n" +
			"    tmp2 as (select item_name, tax from import_information where city = ?)\n" +
			"    select case (select count(*) from tmp1) when 0 then -1 else (\n" +
			"       select case (select count(*) from tmp2) when 0 then -1 else (\n" +
			"            select  sum(tax)/sum(price) as ImportTaxRate from (\n" +
			"            select tmp1.name, tmp1.price, tmp2.tax from tmp1\n" +
			"            inner join tmp2\n" +
			"            on tmp1.name = tmp2.item_name) as foo\n" +
			"       )\n" +
			"       end\n" +
			"    )\n" +
			"    end\n";
	@Override
	public double getImportTaxRate(LogInfo logInfo, String city, String itemType) {
		try {
			if (logInfo.type() != StaffType.CompanyManager || !checkUser(logInfo)) {
				return -1;
			}
			PreparedStatement statement = this.conn.prepareStatement(getImportTaxRateSQL);
			statement.setString(1, itemType);
			statement.setString(2, city);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				return rs.getDouble(1);
			} else {
				return -1;
			}
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return 0;
	}
	
	//Company Manager User


	private static final String getExportTaxRateSQL = "with\n" +
			"    tmp1 as (select name, price from item where type = ?),\n" +
			"    tmp2 as (select item_name, tax from export_information where city = ?)\n" +
			"    select case (select count(*) from tmp1) when 0 then -1 else (\n" +
			"       select case (select count(*) from tmp2) when 0 then -1 else (\n" +
			"            select  sum(tax)/sum(price) as ExportTaxRate from (\n" +
			"            select tmp1.name, tmp1.price, tmp2.tax from tmp1\n" +
			"            inner join tmp2\n" +
			"            on tmp1.name = tmp2.item_name) as foo\n" +
			"       )\n" +
			"       end\n" +
			"    )\n" +
			"    end\n";
	@Override
	public double getExportTaxRate(LogInfo logInfo, String city, String itemType) {
		try {
			if (logInfo.type() != StaffType.CompanyManager || !checkUser(logInfo)) {
				return -1;
			}
			PreparedStatement statement = this.conn.prepareStatement(getExportTaxRateSQL);
			statement.setString(1, itemType);
			statement.setString(2, city);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				return rs.getDouble(1);
			} else {
				return -1;
			}
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return 0;
	}
	private static final String getItemContainerCode = "select code from container where item_name = ?";
	private static final String checkContainerCode = "select case (select count(*) from container where code = ?) when 0 then false else true end";
	private static final String checkPackingContainerSQL = "with\n" +
			"    tmp1 as (select name from item where state = 'Packing to Container' or state = 'Shipping' or state = 'Unpacking from Container'),\n" +
			"    tmp2 as (select item_name from container where code = ? and item_name != ?)\n" +
			"    select case (select count(tmp1.name) from tmp1 inner join tmp2 on tmp1.name = tmp2.item_name) when 0 then true else false end";

	private static final String getContainerTypeSQL = "select type from container where code = ?";

	private static final String updateItemContainerCode = "update container set code = ?, type = ? where item_name = ?";
	//Company Manager User
	@Override
	public boolean loadItemToContainer(LogInfo logInfo, String itemName, String containerCode) {
		try {
			if (logInfo.type() != StaffType.CompanyManager || !checkUser(logInfo)) {
				return false;
			}
			PreparedStatement statement; ResultSet rs;
			statement = this.conn.prepareStatement(checkItemSQL);
			statement.setString(1, itemName);
			rs = statement.executeQuery();
			if (rs.next()) {
				if (rs.getBoolean(1)) return false;
			} else {
				return false;
			}

			statement = this.conn.prepareStatement(getItemStateSQL);
			statement.setString(1, itemName);

			rs = statement.executeQuery();
			if (rs.next()) {
				if (!rs.getString(1).equals("Packing to Container")) return false;
			} else {
				return false;
			}
			statement = this.conn.prepareStatement(getItemContainerCode);
			statement.setString(1, itemName);
			rs = statement.executeQuery();
			String nowContainerCode;
			if (rs.next()) {
				nowContainerCode = rs.getString(1);
			} else {
				return false;
			}

			statement = this.conn.prepareStatement(checkContainerCode);
			statement.setString(1, containerCode);
			rs = statement.executeQuery();
			if (rs.next()) {
				if (!rs.getBoolean(1)) return false;
			} else {
				return false;
			}

			if (nowContainerCode == null) {
				statement = this.conn.prepareStatement(checkPackingContainerSQL);
				statement.setString(1, containerCode);
				statement.setNull(2, Types.VARCHAR);
				rs = statement.executeQuery();
				if (rs.next()) {
					if (!rs.getBoolean(1)) return false;
				} else {
					return false;
				}

				String containerType = null;
				statement = this.conn.prepareStatement(getContainerTypeSQL);
				statement.setString(1, containerCode);
				rs = statement.executeQuery();
				if (rs.next()) {
					containerType = rs.getString(1);
				} else {
					return false;
				}

				statement = this.conn.prepareStatement(updateItemContainerCode);
				statement.setString(1, containerCode);
				statement.setString(2, containerType);
				statement.setString(3, itemName);
				statement.executeUpdate();

			} else {

				statement = this.conn.prepareStatement(checkPackingContainerSQL);
				statement.setString(1, containerCode);
				statement.setString(2, itemName);
				rs = statement.executeQuery();
				if (rs.next()) {
					if (!rs.getBoolean(1)) return false;
				} else {
					return false;
				}

				String containerType = null;
				statement = this.conn.prepareStatement(getContainerTypeSQL);
				statement.setString(1, containerCode);
				rs = statement.executeQuery();
				if (rs.next()) {
					containerType = rs.getString(1);
				} else {
					return false;
				}

				statement = this.conn.prepareStatement(updateItemContainerCode);
				statement.setString(1, containerCode);
				statement.setString(2, containerType);
				statement.setString(3, itemName);
				statement.executeUpdate();

			}

			return true;
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return false;
	}
	
	//Company Manager User
	private static final String getItemNameFromContainerCodeSQL = "with\n" +
			"    tmp1 as (select item_name from container where code = ?),\n" +
			"    tmp2 as (select name from item where state = 'Packing to Container')\n" +
			"    select tmp1.item_name from tmp1 inner join tmp2 on tmp2.name = tmp1.item_name;";
	private static final String checkShipSailing = "with\n" +
			"    tmp1 as (select item_name from ship where ship_name = ?),\n" +
			"    tmp2 as (select name from item where state = 'Shipping')\n" +
			"    select case (select count(*) from tmp1) when 0 then false else (\n" +
			"        select case (select count(tmp1.item_name) from tmp1 inner join tmp2 on tmp1.item_name = tmp2.name)\n" +
			"        when 0 then true else false end\n" +
			"    ) end";
	private static final String getShipCompany = "select company from ship where ship_name = ?";
	private static final String getStaffCompany = "select company from staff where name = ?";
	private static final String getItemCompany = "select company from ship where item_name = ?";
	private static final String updateItemShip = "update ship set ship_name = ? where item_name = ?";
	@Override
	public boolean loadContainerToShip(LogInfo logInfo, String shipName, String containerCode) {
		try {
			if (logInfo.type() != StaffType.CompanyManager || !checkUser(logInfo)) {
				return false;
			}
			PreparedStatement statement; ResultSet rs;
			statement = this.conn.prepareStatement(getItemNameFromContainerCodeSQL);
			statement.setString(1, containerCode);
			rs = statement.executeQuery();
			String itemName = null;
			if (rs.next()) {
				itemName = rs.getString(1);
			} else {
				return false;
			}
			statement = this.conn.prepareStatement(getItemStateSQL);
			statement.setString(1, itemName);
			rs = statement.executeQuery();
			String itemState = null;
			if (rs.next()) {
				itemState = rs.getString(1);
			} else {
				return false;
			}
			if (!itemState.equals("Packing to Container")) return false;
			statement = this.conn.prepareStatement(checkShipSailing);
			statement.setString(1, shipName);
			rs = statement.executeQuery();
			if (rs.next()) {
				if (!rs.getBoolean(1)) return false;
			} else {
				return false;
			}
			String shipCompany = null;
			statement = this.conn.prepareStatement(getShipCompany);
			statement.setString(1, shipName);
			rs = statement.executeQuery();
			if (rs.next()) {
				shipCompany = rs.getString(1);
			} else {
				return false;
			}
			String staffCompany = null;
			statement = this.conn.prepareStatement(getStaffCompany);
			statement.setString(1, logInfo.name());
			rs = statement.executeQuery();
			if (rs.next()) {
				staffCompany = rs.getString(1);
			} else {
				return false;
			}
			if (!shipCompany.equals(staffCompany)) return false;
			String itemCompany = null;
			statement = this.conn.prepareStatement(getItemCompany);
			statement.setString(1, itemName);
			rs = statement.executeQuery();
			if (rs.next()) {
				itemCompany = rs.getString(1);
			} else {
				return false;
			}
			if (!itemCompany.equals(staffCompany)) return false;
			statement = this.conn.prepareStatement(updateItemState);
			statement.setString(1, "Waiting for Shipping");
			statement.setString(2, itemName);
			statement.executeUpdate();

			statement = this.conn.prepareStatement(updateItemShip);
			statement.setString(1, shipName);
			statement.setString(2, itemName);
			statement.executeUpdate();
			return true;
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return false;
	}
	
	//Company Manager User
	private static final String getWaitingShippingItems = "with\n" +
			"    tmp1 as (select item_name from ship where ship_name = ? and company = ?),\n" +
			"    tmp2 as (select name from item where state = 'Waiting for Shipping')\n" +
			"    select tmp1.item_name from tmp1 inner join tmp2 on tmp2.name = tmp1.item_name";
	@Override
	public boolean shipStartSailing(LogInfo logInfo, String shipName) {
		try {
			if (logInfo.type() != StaffType.CompanyManager || !checkUser(logInfo)) {
				return false;
			}
			PreparedStatement statement; ResultSet rs;
			statement = this.conn.prepareStatement(checkShipSailing);
			statement.setString(1, shipName);
			rs = statement.executeQuery();
			if (rs.next()) {
				if (!rs.getBoolean(1)) return false;
			} else {
				return false;
			}
			statement = this.conn.prepareStatement(getStaffCompany);
			statement.setString(1, logInfo.name());
			rs = statement.executeQuery();
			String staffCompany = null;
			if (rs.next()) {
				staffCompany = rs.getString(1);
			} else {
				return false;
			}
			statement = this.conn.prepareStatement(getWaitingShippingItems);
			statement.setString(1, shipName);
			statement.setString(2, staffCompany);
			rs = statement.executeQuery();
			ArrayList<String> list = new ArrayList<>();
			if (rs.next()) {
				do {
					list.add(rs.getString(1));
				} while (rs.next());
			} else {
				return false;
			}
			for (String item : list) {
				statement = this.conn.prepareStatement(updateItemState);
				statement.setString(1, "Shipping");
				statement.setString(2, item);
				statement.executeUpdate();
			}
			return true;
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return false;
	}
	
	//Company Manager User
	@Override
	public boolean unloadItem(LogInfo logInfo, String item) {
		try {
			if (logInfo.type() != StaffType.CompanyManager || !checkUser(logInfo)) {
				return false;
			}
			PreparedStatement statement; ResultSet rs;
			statement = this.conn.prepareStatement(getItemStateSQL);
			statement.setString(1, item);
			rs = statement.executeQuery();
			if (rs.next()) {
				if (!rs.getString(1).equals("Shipping")) return false;
			} else {
				return false;
			}
			statement = this.conn.prepareStatement(updateItemState);
			statement.setString(1, "Unpacking from Container");
			statement.setString(2, item);
			statement.executeUpdate();
			return true;
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return false;
	}
	
	//Company Manager User
	@Override
	public boolean itemWaitForChecking(LogInfo logInfo, String item) {
		try {
			if (logInfo.type() != StaffType.CompanyManager || !checkUser(logInfo)) {
				return false;
			}
			PreparedStatement statement; ResultSet rs;
			statement = this.conn.prepareStatement(getItemStateSQL);
			statement.setString(1, item);
			rs = statement.executeQuery();
			if (rs.next()) {
				if (!rs.getString(1).equals("Unpacking from Container")) return false;
			} else {
				return false;
			}
			statement = this.conn.prepareStatement(updateItemState);
			statement.setString(1, "Import Checking");
			statement.setString(2, item);
			statement.executeUpdate();
			return true;
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return false;
	}

	public int stateToInt(ItemState itemState) {
		if (itemState == null) return 0;
		switch (itemState) {
			case PickingUp: return 1;
			case ToExportTransporting: return 2;
			case ExportChecking: return 3;
			case ExportCheckFailed: return 4;
			case PackingToContainer: return 5;
			case WaitingForShipping: return 6;
			case Shipping: return 7;
			case UnpackingFromContainer: return 8;
			case ImportChecking: return 9;
			case ImportCheckFailed: return 10;
			case FromImportTransporting: return 11;
			case Delivering: return 12;
			case Finish: return 13;
			default: return 0;
		}
	}
	//Courier User
	private static final String checkItemSQL = "select case (select count(*) from item where name = ?)\n" +
			"    when 0 then true\n" +
			"    else false\n" +
			"end\n";
	private static final String checkContainerSQL = "select * from container where item_name = ?";
	private static final String checkShipSQL = "select * from ship where item_name = ?";

	private static final String checkStaffCity = "select case (select count(*) from staff where name = ? and city = ?)\n" +
			"	when 0 then false\n" +
			"	else true\n" +
			"end\n";

	public boolean checkItem(ItemInfo itemInfo, boolean isNewItem, String retrievalCourier) {
		if (itemInfo == null) return false;
		try {
			PreparedStatement statement;
			statement = this.conn.prepareStatement(checkItemSQL);
			statement.setString(1, itemInfo.name());
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				if (rs.getBoolean(1) == false) return false;
			} else {
				return false;
			}
			if (itemInfo.name() == null) return false;
			if (itemInfo.$class() == null) return false;
			if (!isNewItem) {
				if (itemInfo.retrieval() == null) return false;
				if (itemInfo.retrieval().city() == null) return false;
				if (itemInfo.retrieval().courier() == null) return false;
			} else {
				if (itemInfo.retrieval() == null) return false;
				if (itemInfo.retrieval().city() == null) return false;
				if (itemInfo.retrieval().courier() != null && !itemInfo.retrieval().courier().equals(retrievalCourier))
					return false;
				statement = this.conn.prepareStatement(checkStaffCity);
				statement.setString(1, retrievalCourier);
				statement.setString(2, itemInfo.retrieval().city());
				rs = statement.executeQuery();
				if (rs.next()) {
					if (rs.getBoolean(1) == false) return false;
				} else return false;
			}

			if (itemInfo.delivery() == null) return false;
			if (itemInfo.delivery().city() == null) return false;
			if (itemInfo.delivery().courier() == null && stateToInt(itemInfo.state()) >= 11) return false;
			if (itemInfo.delivery().courier() != null) {
				statement = this.conn.prepareStatement(checkStaffCity);
				statement.setString(1, itemInfo.delivery().courier());
				statement.setString(2, itemInfo.delivery().city());
				rs = statement.executeQuery();
				if (rs.next()) {
					if (rs.getBoolean(1) == false) return false;
				} else return false;
			}

			if (itemInfo.export() == null) return false;
			if (itemInfo.export().city() == null) return false;
			if (itemInfo.export().officer() == null && stateToInt(itemInfo.state()) >= 3) return false;
			if (itemInfo.export().officer() != null) {
				statement = this.conn.prepareStatement(checkStaffCity);
				statement.setString(1, itemInfo.export().officer());
				statement.setString(2, itemInfo.export().city());
				rs = statement.executeQuery();
				if (rs.next()) {
					if (rs.getBoolean(1) == false) return false;
				} else return false;
			}

			if (itemInfo.$import() == null) return false;
			if (itemInfo.$import().city() == null) return false;
			if (itemInfo.$import().officer() == null && stateToInt(itemInfo.state()) >= 9) return false;
			if (itemInfo.$import().officer() != null) {
				statement = this.conn.prepareStatement(checkStaffCity);
				statement.setString(1, itemInfo.$import().officer());
				statement.setString(2, itemInfo.$import().city());
				rs = statement.executeQuery();
				if (rs.next()) {
					if (rs.getBoolean(1) == false) return false;
				} else return false;
			}

			if (itemInfo.$import().city().equals(itemInfo.export().city())) return false;

			double exportTaxRate = itemInfo.export().tax() / itemInfo.price();
			double importTaxRate = itemInfo.$import().tax() / itemInfo.price();

			statement = this.conn.prepareStatement(getImportTaxRateSQL);
			statement.setString(1, itemInfo.$class());
			statement.setString(2, itemInfo.$import().city());
			rs = statement.executeQuery();
			if (rs.next()) {
				double RealImportTaxRate = rs.getDouble(1);
				if (Math.abs(RealImportTaxRate + 1) > 1e-7 && Math.abs(RealImportTaxRate - importTaxRate) > 1e-7) return false;
			} else {
				return false;
			}
			statement = this.conn.prepareStatement(getExportTaxRateSQL);
			statement.setString(1, itemInfo.$class());
			statement.setString(2, itemInfo.export().city());
			rs = statement.executeQuery();
			if (rs.next()) {
				double RealExportTaxRate = rs.getDouble(1);
				if (Math.abs(RealExportTaxRate + 1) > 1e-7 && Math.abs(RealExportTaxRate - exportTaxRate) > 1e-7) return false;
			} else {
				return false;
			}

			String containerCode = null, containerType = null;
			statement = this.conn.prepareStatement(checkContainerSQL);
			statement.setString(1, itemInfo.name());
			rs = statement.executeQuery();
			if (rs.next()) {
				containerCode = rs.getString(2);
				containerType = rs.getString(3);
				if (containerCode == null && containerType != null) return false;
				if (containerCode != null && containerType == null) return false;
				if (containerCode == null && containerType == null && stateToInt(itemInfo.state()) >= 5) return false;
			} else {
				if (stateToInt(itemInfo.state()) >= 5) return false;
			}
			String shipName = null, company = null;
			statement = this.conn.prepareStatement(checkShipSQL);
			statement.setString(1, itemInfo.name());
			rs = statement.executeQuery();
			if (rs.next()) {
				shipName = rs.getString(2);
				company = rs.getString(3);
				if (company == null) return false;
				if (shipName == null && stateToInt(itemInfo.state()) >= 7) return false;
			} else {
				if (stateToInt(itemInfo.state()) >= 7) return false;
			}
		} catch (SQLException e) {
			return false;
		}
		return true;
	}
	private static final String insertNewItemSQL2 = "insert into delivery_information (item_name, city, staff_name) values (?, ?, ?)";
	private static final String insertNewItemSQL3 = "insert into export_information (item_name, city, tax, staff_name) values (?, ?, ?, ?)";
	private static final String insertNewItemSQL4 = "insert into import_information (item_name, city, tax, staff_name) values (?, ?, ?, ?)";
	private static final String insertNewItemSQL5 = "insert into item (name, type, price, state) values (?, ?, ?, ?)";
	private static final String insertNewItemSQL6 = "insert into retrieval_information (item_name, city, staff_name) values (?, ?, ?)";

	@Override
	public boolean newItem(LogInfo logInfo, ItemInfo itemInfo) {
		try {
			if (logInfo.type() != StaffType.Courier || !checkUser(logInfo)) {
				return false;
			}
			if (checkItem(itemInfo, true, logInfo.name()) == false) return false;
			if (stateToInt(itemInfo.state()) > 1) return false;
			PreparedStatement statement;
			statement = this.conn.prepareStatement(insertNewItemSQL5);
			statement.setString(1, itemInfo.name());
			statement.setString(2, itemInfo.$class());
			statement.setDouble(3, itemInfo.price());
			statement.setString(4, "Picking-up");
			statement.execute();

			statement = this.conn.prepareStatement(insertNewItemSQL2);
			statement.setString(1, itemInfo.name());
			if (itemInfo == null) {
				statement.setNull(2, Types.VARCHAR);
				statement.setNull(3, Types.VARCHAR);
			} else {
				if (itemInfo.delivery().city() != null)
					statement.setString(2, itemInfo.delivery().city());
				else
					statement.setNull(2, Types.VARCHAR);
				if (itemInfo.delivery().courier() != null)
					statement.setString(3, itemInfo.delivery().courier());
				else
					statement.setNull(3, Types.VARCHAR);
			}
			statement.execute();

			statement = this.conn.prepareStatement(insertNewItemSQL3);
			statement.setString(1, itemInfo.name());

			if (itemInfo.export().city() != null) statement.setString(2, itemInfo.export().city());
			else statement.setNull(2, Types.VARCHAR);
			statement.setDouble(3, itemInfo.export().tax());
			if (itemInfo.export().officer() != null) statement.setString(4, itemInfo.export().officer());
			else statement.setNull(4, Types.VARCHAR);
			statement.execute();


			statement = this.conn.prepareStatement(insertNewItemSQL4);
			statement.setString(1, itemInfo.name());

			statement.setString(2, itemInfo.$import().city());
			statement.setDouble(3, itemInfo.$import().tax());
			if (itemInfo.$import().officer() != null)
				statement.setString(4, itemInfo.$import().officer());
			else
				statement.setNull(4, Types.VARCHAR);
			statement.execute();



			statement = this.conn.prepareStatement(insertNewItemSQL6);
			statement.setString(1, itemInfo.name());
			statement.setString(2, itemInfo.retrieval().city());
			statement.setString(3, logInfo.name());
			statement.execute();

			return true;
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}

		return false;
	}

	//Courier User
	private ItemState stringToItemState(String s) {
		if (s.equals("Delivering")) return ItemState.Delivering;
		else if (s.equals("Export Check Fail")) return ItemState.ExportCheckFailed;
		else if (s.equals("Export Checking")) return ItemState.ExportChecking;
		else if (s.equals("Finish")) return ItemState.Finish;
		else if (s.equals("From-Import Transporting")) return ItemState.FromImportTransporting;
		else if (s.equals("Import Check Fail")) return ItemState.ImportCheckFailed;
		else if (s.equals("Import Checking")) return ItemState.ImportChecking;
		else if (s.equals("Packing to Container")) return ItemState.PackingToContainer;
		else if (s.equals("Picking-up")) return ItemState.PickingUp;
		else if (s.equals("Shipping")) return ItemState.Shipping;
		else if (s.equals("To-Export Transporting")) return ItemState.ToExportTransporting;
		else if (s.equals("Unpacking from Container")) return ItemState.UnpackingFromContainer;
		else if (s.equals("Waiting for Shipping")) return ItemState.WaitingForShipping;
		return null;
	}
	private static final String getItemStateSQL = "select state from item where name = ?";
	private static final String getItemRetrievalCourier = "select staff_name from retrieval_information where item_name = ?";
	private static final String getItemDeliveryCourierAndCity = "select staff_name, city from delivery_information where item_name = ?";
	private static final String updateItemState = "update item set state = ? where name = ?";
	private static final String updateItemDeliveryCourier = "update delivery_information set staff_name = ? where item_name = ?";
	private static final String checkItemDeliveryCourier = "select case \n" +
			"(select count(*) from staff where name = ? and city = ?) when 0 then false\n" +
			"else true end";
	@Override
	public boolean setItemState(LogInfo logInfo, String name, ItemState s) {
		try {
			PreparedStatement statement;
			if (logInfo.type() != StaffType.Courier || !checkUser(logInfo)) {
				return false;
			}
			statement = this.conn.prepareStatement(checkItemSQL);
			statement.setString(1, name);
			ResultSet rs = statement.executeQuery();
			if (rs.next()) {
				if (rs.getBoolean(1) == true) return false;
			} else {
				return false;
			}
			statement = this.conn.prepareStatement(getItemStateSQL);
			statement.setString(1, name);
			rs = statement.executeQuery();
			String state = null;
			if (rs.next()) {
				state = rs.getString(1);
			} else {
				return false;
			}
			if (state.equals("Picking-up")) {
				String RetrievalCourier = null;
				statement = this.conn.prepareStatement(getItemRetrievalCourier);
				statement.setString(1, name);
				rs = statement.executeQuery();
				if (rs.next()) {
					RetrievalCourier = rs.getString(1);
					if (RetrievalCourier == null) return false;
					if (!RetrievalCourier.equals(logInfo.name())) return false;
					if (s != ItemState.ToExportTransporting) return false;
					statement = this.conn.prepareStatement(updateItemState);
					statement.setString(1, "To-Export Transporting");
					statement.setString(2, name);
					statement.executeUpdate();
				} else {
					return false;
				}
			} else if (state.equals("To-Export Transporting")) {
				String RetrievalCourier = null;
				statement = this.conn.prepareStatement(getItemRetrievalCourier);
				statement.setString(1, name);
				rs = statement.executeQuery();
				if (rs.next()) {
					RetrievalCourier = rs.getString(1);
					if (RetrievalCourier == null) return false;
					if (!RetrievalCourier.equals(logInfo.name())) return false;
					if (s != ItemState.ExportChecking) return false;
					statement = this.conn.prepareStatement(updateItemState);
					statement.setString(1, "Export Checking");
					statement.setString(2, name);
					statement.executeUpdate();
				} else {
					return false;
				}
			} else if (state.equals("From-Import Transporting")) {
				String deliveryCourier = null, deliveryCity = null;
				statement = this.conn.prepareStatement(getItemDeliveryCourierAndCity);
				statement.setString(1, name);
				rs = statement.executeQuery();
				if (rs.next()) {
					deliveryCourier = rs.getString(1);
					deliveryCity = rs.getString(2);
					if (deliveryCourier == null) {
						statement = this.conn.prepareStatement(checkItemDeliveryCourier);
						statement.setString(1, logInfo.name());
						statement.setString(2, deliveryCity);
						rs = statement.executeQuery();
						if (rs.next()) {
							if (rs.getBoolean(1) == false) return false;
							statement = this.conn.prepareStatement(updateItemDeliveryCourier);
							statement.setString(1, logInfo.name());
							statement.setString(2, name);
							statement.executeUpdate();
						} else {
							return false;
						}
					} else {
						if (!logInfo.name().equals(deliveryCourier)) return false;
						if (s != ItemState.Delivering) return false;
						statement = this.conn.prepareStatement(updateItemState);
						statement.setString(1, "Delivering");
						statement.setString(2, name);
						statement.executeUpdate();
					}
				} else {
					return false;
				}
			} else if (state.equals("Delivering")) {
				String deliveryCourier = null;
				statement = this.conn.prepareStatement(getItemDeliveryCourierAndCity);
				statement.setString(1, name);
				rs = statement.executeQuery();
				if (rs.next()) {
					deliveryCourier = rs.getString(1);
					if (deliveryCourier == null) return false;
					if (!deliveryCourier.equals(logInfo.name())) return false;
					if (s != ItemState.Finish) return false;
					statement = this.conn.prepareStatement(updateItemState);
					statement.setString(1, "Finish");
					statement.setString(2, name);
					statement.executeUpdate();
				} else {
					return false;
				}
			} else {
				return false;
			}
			return true;
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}

		return false;
	}
	
	//Seaport Officer User
	private static final String getStaffCity = "select city from staff where name = ?";
	private static final String getItemAtPort = "(select name from item where state = 'Import Checking'\n" +
			"INTERSECT (select item_name from import_information where city = ?))\n" +
			"union\n" +
			"(select name from item where state = 'Export Checking'\n" +
			"INTERSECT (select item_name from export_information where city = ?))";
	@Override
	public String[] getAllItemsAtPort(LogInfo logInfo) {
		try {
			if (logInfo.type() != StaffType.SeaportOfficer || !checkUser(logInfo)) {
				return new String[0];
			}
			List<String> res = new ArrayList<>();

			PreparedStatement statement; ResultSet rs;
			statement = this.conn.prepareStatement(getStaffCity);
			statement.setString(1, logInfo.name());
			rs = statement.executeQuery();

			String staffCity = "";

			if (rs.next()) {
				staffCity = rs.getString(1);
			} else {
				return new String[0];
			}

			statement = this.conn.prepareStatement(getItemAtPort);
			statement.setString(1, staffCity);
			statement.setString(2, staffCity);
			rs = statement.executeQuery();
			if (rs.next()) {
				do {
					res.add(rs.getString(1));
				} while (rs.next());
			} else {
				return new String[0];
			}

			String[] ans = new String[res.size()];
			for (int i = 0; i < res.size(); i++) {
				ans[i] = res.get(i);
			}
			return ans;
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
		return new String[0];
	}
	
	//Seaport Officer User
	private static final String getItemExportStaff = "select staff_name from export_information where item_name = ?";
	private static final String updateItemExportStaff = "update export_information set staff_name = ? where item_name = ?";
	private static final String getItemImportStaff = "select staff_name from import_information where item_name = ?";
	private static final String updateItemImportStaff = "update import_information set staff_name = ? where item_name = ?";
	@Override
	public boolean setItemCheckState(LogInfo logInfo, String itemName, boolean success) {
		try {
			if (logInfo.type() != StaffType.SeaportOfficer || !checkUser(logInfo)) {
				return false;
			}
			PreparedStatement statement; ResultSet rs;
			statement = this.conn.prepareStatement(getItemStateSQL);
			statement.setString(1, itemName);
			rs = statement.executeQuery();
			String nowItemState = null;
			if (rs.next()) {
				nowItemState = rs.getString(1);
			} else {
				return false;
			}
			if (nowItemState.equals("Export Checking")) {
				statement = this.conn.prepareStatement(getItemExportStaff);
				statement.setString(1, itemName);
				String itemStaff = null;
				rs = statement.executeQuery();
				if (rs.next()) {
					itemStaff = rs.getString(1);
				} else {
					return false;
				}

				if (itemStaff != null && !itemStaff.equals(logInfo.name())) return false;

				if (success) {
					statement = this.conn.prepareStatement(updateItemState);
					statement.setString(1, "Packing to Container");
					statement.setString(2, itemName);
					statement.execute();
				} else {
					statement = this.conn.prepareStatement(updateItemState);
					statement.setString(1, "Export Check Fail");
					statement.setString(2, itemName);
					statement.execute();
				}

				if (itemStaff == null) {
					statement = this.conn.prepareStatement(updateItemExportStaff);
					statement.setString(1, logInfo.name());
					statement.setString(2, itemName);
					statement.execute();
				}

			} else if (nowItemState.equals("Import Checking")) {
				statement = this.conn.prepareStatement(getItemImportStaff);
				statement.setString(1, itemName);
				String itemStaff = null;
				rs = statement.executeQuery();
				if (rs.next()) {
					itemStaff = rs.getString(1);
				} else {
					return false;
				}

				if (itemStaff != null && !itemStaff.equals(logInfo.name())) return false;

				if (success) {
					statement = this.conn.prepareStatement(updateItemState);
					statement.setString(1, "From-Import Transporting");
					statement.setString(2, itemName);
					statement.execute();
				} else {
					statement = this.conn.prepareStatement(updateItemState);
					statement.setString(1, "Import Check Fail");
					statement.setString(2, itemName);
					statement.execute();
				}

				if (itemStaff == null) {
					statement = this.conn.prepareStatement(updateItemImportStaff);
					statement.setString(1, logInfo.name());
					statement.setString(2, itemName);
					statement.execute();
				}
			} else {
				return false;
			}
			return true;
		} catch (SQLException e) {
			Main.getThrowableHandler().feedBackThrowable(e);
		}
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
