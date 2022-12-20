package main;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.UUID;

import main.interfaces.LogInfo;
import main.interfaces.StaffInfo;
import main.packet.Packet;
import main.packet.PacketManager;
import main.packet.client.CityCountPacket;
import main.packet.client.CompanyCountPacket;
import main.packet.client.CourierCountPacket;
import main.packet.client.LoginPacket;
import main.packet.client.ShipCountPacket;
import main.packet.server.CityCountInfoPacket;
import main.packet.server.CompanyCountInfoPacket;
import main.packet.server.CourierCountInfoPacket;
import main.packet.server.LoginInfoPacket;
import main.packet.server.ShipCountInfoPacket;

public class Main {
	private static String url = "localhost:5432/cslab1";
	private static String user = "test";
	private static String pass = "123456";
	
	private static HashMap<UUID, LogInfo> session;
	
	private static ThrowableHandler th;

	public static void main(String[] args) throws IOException {
		
		DBManipulation databaseManager = new DBManipulation(url, user, pass);
		ServerSocket serverSocket = new ServerSocket();
		//serverSocket.bind(new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), 23333));
		serverSocket.bind(new InetSocketAddress("localhost", 23333));
		System.out.println("Server has been bound to 23333");
		th = new ThrowableHandler();
		session = new HashMap<>();
		while (true) {
			Socket socket = serverSocket.accept();
			System.out.println("Receive socket...");
			BufferedInputStream input = new BufferedInputStream(socket.getInputStream());
			byte[] bytes = new byte[1024 * 8];
			int len;
			if ((len = input.read(bytes)) != -1) {
				Packet packet = PacketManager.getInstance().receivePacket(len, bytes);
				Packet backPacket = null;
				if (packet instanceof LoginPacket) {
					LoginPacket lp = (LoginPacket) packet;
					String user = lp.getUser();
					String password = lp.getPassword();
					StaffInfo info = databaseManager.getStaffInfoByRoot(user);
					if (info == null || !info.basicInfo().password().equals(password)) {
						backPacket = new LoginInfoPacket(false, null, null, null, null, null, false, null, 0);
					} else {
						UUID newSession = UUID.randomUUID();
						backPacket = new LoginInfoPacket(true, newSession.toString(), user, info.basicInfo().type().toString(), info.city(), info.company(), info.isFemale(), info.phoneNumber(), info.age());
						session.put(newSession, info.basicInfo());
					}
				}
				if (packet instanceof CompanyCountPacket) {
					CompanyCountPacket ccp = (CompanyCountPacket) packet;
					LogInfo info = session.get(UUID.fromString(ccp.getCookie()));
					int count = -1;
					if (info != null) {
						count = databaseManager.getCompanyCount(info);
					}
					backPacket = new CompanyCountInfoPacket(count);
				}
				if (packet instanceof CityCountPacket) {
					CityCountPacket ccp = (CityCountPacket) packet;
					LogInfo info = session.get(UUID.fromString(ccp.getCookie()));
					int count = -1;
					if (info != null) {
						count = databaseManager.getCityCount(info);
					}
					backPacket = new CityCountInfoPacket(count);
				}
				if (packet instanceof CourierCountPacket) {
					CourierCountPacket ccp = (CourierCountPacket) packet;
					LogInfo info = session.get(UUID.fromString(ccp.getCookie()));
					int count = -1;
					if (info != null) {
						count = databaseManager.getCourierCount(info);
					}
					backPacket = new CourierCountInfoPacket(count);
				}
				if (packet instanceof ShipCountPacket) {
					ShipCountPacket scp = (ShipCountPacket) packet;
					LogInfo info = session.get(UUID.fromString(scp.getCookie()));
					int count = -1;
					if (info != null) {
						count = databaseManager.getShipCount(info);
					}
					backPacket = new ShipCountInfoPacket(count);
				}
				BufferedOutputStream writer = new BufferedOutputStream(socket.getOutputStream());
				writer.write((backPacket.getCode() + "@" + backPacket.getContext()).getBytes());
				writer.flush();
				writer.close();
			}
			input.close();
			socket.close();
				
		}
	}
	
	protected static ThrowableHandler getThrowableHandler() {
		return th;
	}
	
}
