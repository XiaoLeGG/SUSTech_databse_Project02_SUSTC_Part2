import cs307.project2.interfaces.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Calendar;
import java.util.Scanner;

public class DatabaseManager implements IDatabaseManipulation {
    String database, root, pass;
    public DatabaseManager(String database, String root, String pass) {
        this.database = database; this.root = root; this.pass = pass;
        Connection con;
        try {
            Class.forName("org.postgresql.Driver");
            con = DriverManager.getConnection(database, root, pass);
            System.out.println("数据库成功链接！");
            Statement sta = con.createStatement();
            sta.executeUpdate("create table if not exists staff (" +
                    "    name varchar not null," +
                    "    password varchar not null," +
                    "    type varchar not null," +
                    "    city varchar," +
                    "    gender boolean not null," +
                    "    phone_number varchar not null," +
                    "    birth_year integer not null," +
                    "    company varchar," +
                    "    primary key (name)" +
                    ");" +
                    "create table if not exists export_information (" +
                    "    item_name varchar not null," +
                    "    city varchar not null," +
                    "    tax numeric(20, 7) not null," +
                    "    staff_name varchar references staff(name)," +
                    "    primary key (item_name)" +
                    ");" +
                    "create table if not exists import_information(" +
                    "    item_name varchar not null," +
                    "    city varchar not null," +
                    "    tax numeric(20, 7) not null," +
                    "    staff_name varchar references staff(name)," +
                    "    primary key (item_name)" +
                    ");" +
                    "create table if not exists ship(" +
                    "    item_name varchar not null," +
                    "    ship_name varchar," +
                    "    company varchar not null," +
                    "    primary key (item_name)" +
                    ");" +
                    "create table if not exists container(" +
                    "    item_name varchar not null," +
                    "    code varchar," +
                    "    type varchar," +
                    "    primary key (item_name)" +
                    ");" +
                    "create table if not exists retrieval_information(" +
                    "    item_name varchar not null," +
                    "    city varchar not null," +
                    "    staff_name varchar not null references staff(name)," +
                    "    primary key (item_name)" +
                    ");" +
                    "create table if not exists delivery_information(" +
                    "    item_name varchar not null," +
                    "    city varchar not null," +
                    "    staff_name varchar references staff(name)," +
                    "    primary key (item_name)" +
                    ");" +
                    "create table if not exists item(" +
                    "    name varchar not null," +
                    "    type varchar not null," +
                    "    price numeric(20, 7) not null," +
                    "    state varchar not null," +
                    "    primary key (name)" +
                    ");" +
                    "alter table delivery_information add constraint  ForeignKey_DeliveryInformation_ItemName foreign key (item_name) references item(name);" +
                    "alter table retrieval_information add constraint  ForeignKey_RetrievalInformation_ItemName foreign key (item_name) references item(name);" +
                    "alter table export_information add constraint  ForeignKey_ExportInformation_ItemName foreign key (item_name) references item(name);" +
                    "alter table import_information add constraint  ForeignKey_ImportInformation_ItemName foreign key (item_name) references item(name);" +
                    "alter table ship add constraint ForeignKey_Ship_ItemName foreign key (item_name) references item(name);" +
                    "alter table container add constraint ForeignKey_Container_ItemName foreign key (item_name) references item(name);");
            con.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
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
        Connection con;
        try {
            Class.forName("org.postgresql.Driver");
            con = DriverManager.getConnection(database, root, pass);
            con.prepareStatement("ALTER TABLE delivery_information DISABLE TRIGGER ALL").execute();
            con.prepareStatement("ALTER TABLE retrieval_information DISABLE TRIGGER ALL").execute();
            con.prepareStatement("ALTER TABLE export_information DISABLE TRIGGER ALL").execute();
            con.prepareStatement("ALTER TABLE import_information DISABLE TRIGGER ALL").execute();
            con.prepareStatement("ALTER TABLE ship DISABLE TRIGGER ALL").execute();
            con.prepareStatement("ALTER TABLE container DISABLE TRIGGER ALL").execute();
            Scanner scanner = new Scanner(new File(staffsCSV));
            Scanner valueScanner = null;
            con.setAutoCommit(false);
            int index = 0;
            Calendar cal = Calendar.getInstance();
            int now_year = cal.get(Calendar.YEAR);
            String password = null, type = null, city = null, phone_number = null, company = null, name = null;
            int birth_year = 0;
            boolean gender = false;
            PreparedStatement statement = con.prepareStatement(staffSQL);
            scanner.nextLine();
            while (scanner.hasNextLine()) {
                valueScanner = new Scanner(scanner.nextLine());
                valueScanner.useDelimiter(",");
                while (valueScanner.hasNext()) {
                    String data = valueScanner.next();
                    if (data.equals("")) data = null;
                    if (index == 0) name = data;
                    else if (index == 1) type = data;
                    else if (index == 2) company = data;
                    else if (index == 3) city = data;
                    else if (index == 4) {
                        if (data.equals("female")) gender = true;
                        else gender = false;
                    }
                    else if (index == 5) birth_year = now_year - Integer.parseInt(data);
                    else if (index == 6) phone_number = data;
                    else if (index == 7) password = data;
                    index++;
                }
                index = 0;
                statement.setString(1, name);
                statement.setString(2, password);
                statement.setString(3, type);
                statement.setString(4, city);
                statement.setBoolean(5, gender);
                statement.setString(6, phone_number);
                statement.setInt(7, birth_year);
                statement.setString(8, company);
                statement.addBatch();
            }
            statement.executeBatch();
            scanner = new Scanner(new File(recordsCSV));
            valueScanner = null;
            String itemName = null,itemClass = null;
            double itemPrice = 0;
            String retrievalCity = null,retrievalCourier = null ,deliveryCity = null,deliveryCourier= null,exportCity = null,importCity = null;
            double exportTax = 0, importTax = 0;
            String exportOfficer = null, importOfficer = null, containerCode = null, containerType = null, shipName = null, companyName = null, itemState = null;
            PreparedStatement containerStatement = con.prepareStatement(containerSQL);
            PreparedStatement deliveryInformationStatement = con.prepareStatement(deliveryInformationSQL);
            PreparedStatement retrievalInformationStatement = con.prepareStatement(retrievalInformationSQL);
            PreparedStatement exportInformationStatement = con.prepareStatement(exportInformationSQL);
            PreparedStatement importInformationStatement = con.prepareStatement(importInformationSQL);
            PreparedStatement itemStatement = con.prepareStatement(itemSQL);
            PreparedStatement shipStatement = con.prepareStatement(shipSQL);
            scanner.nextLine();
            int cnt = 0;
            String data = null;
            while (scanner.hasNextLine()) {
                valueScanner = new Scanner(scanner.nextLine());
                valueScanner.useDelimiter(",");
                while (valueScanner.hasNext()) {
                    data = valueScanner.next();
                    if (data.equals("")) data = null;
                    if (index == 0) itemName =data;
                    else if (index == 1) itemClass = data;
                    else if (index == 2) itemPrice = Double.parseDouble(data);
                    else if (index == 3) retrievalCity = data;
                    else if (index == 4) retrievalCourier = data;
                    else if (index == 5) deliveryCity = data;
                    else if (index == 6) deliveryCourier = data;
                    else if (index == 7) exportCity = data;
                    else if (index == 8) importCity = data;
                    else if (index == 9) exportTax = Double.parseDouble(data);
                    else if (index == 10) importTax = Double.parseDouble(data);
                    else if (index == 11) exportOfficer = data;
                    else if (index == 12) importOfficer = data;
                    else if (index == 13) containerCode = data;
                    else if (index == 14) containerType = data;
                    else if (index == 15) shipName = data;
                    else if (index == 16) companyName = data;
                    else if (index == 17) itemState = data;
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
            con.commit();
            con.setAutoCommit(true);
            con.prepareStatement("ALTER TABLE delivery_information ENABLE TRIGGER ALL").execute();
            con.prepareStatement("ALTER TABLE retrieval_information ENABLE TRIGGER ALL").execute();
            con.prepareStatement("ALTER TABLE export_information ENABLE TRIGGER ALL").execute();
            con.prepareStatement("ALTER TABLE import_information ENABLE TRIGGER ALL").execute();
            con.prepareStatement("ALTER TABLE ship ENABLE TRIGGER ALL").execute();
            con.prepareStatement("ALTER TABLE container ENABLE TRIGGER ALL").execute();
            con.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("Total Time = " + (end - start) + " ms");
    }
    @Override
    public double getImportTaxRate(LogInfo logInfo, String s, String s1) {
        return 0;
    }

    @Override
    public double getExportTaxRate(LogInfo logInfo, String s, String s1) {
        return 0;
    }

    @Override
    public boolean loadItemToContainer(LogInfo logInfo, String s, String s1) {
        return false;
    }

    @Override
    public boolean loadContainerToShip(LogInfo logInfo, String s, String s1) {
        return false;
    }

    @Override
    public boolean shipStartSailing(LogInfo logInfo, String s) {
        return false;
    }

    @Override
    public boolean unloadItem(LogInfo logInfo, String s) {
        return false;
    }

    @Override
    public boolean itemWaitForChecking(LogInfo logInfo, String s) {
        return false;
    }

    @Override
    public boolean newItem(LogInfo logInfo, ItemInfo itemInfo) {
        return false;
    }

    @Override
    public boolean setItemState(LogInfo logInfo, String s, ItemState itemState) {
        return false;
    }

    @Override
    public String[] getAllItemsAtPort(LogInfo logInfo) {
        return new String[0];
    }

    @Override
    public boolean setItemCheckState(LogInfo logInfo, String s, boolean b) {
        return false;
    }

    @Override
    public int getCompanyCount(LogInfo logInfo) {
        return 0;
    }

    @Override
    public int getCityCount(LogInfo logInfo) {
        return 0;
    }

    @Override
    public int getCourierCount(LogInfo logInfo) {
        return 0;
    }

    @Override
    public int getShipCount(LogInfo logInfo) {
        return 0;
    }

    @Override
    public ItemInfo getItemInfo(LogInfo logInfo, String s) {
        return null;
    }

    @Override
    public ShipInfo getShipInfo(LogInfo logInfo, String s) {
        return null;
    }

    @Override
    public ContainerInfo getContainerInfo(LogInfo logInfo, String s) {
        return null;
    }

    @Override
    public StaffInfo getStaffInfo(LogInfo logInfo, String s) {
        return null;
    }
}
