package main.packet;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

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



public class PacketManager {
	
	private static PacketManager manager = new PacketManager();
	private HashMap<Integer, Class<? extends Packet>> packetCodes;
	
	private PacketManager() {
		this.init();
	}
	
	private void init() {
		packetCodes = new HashMap<>();
		packetCodes.put(LoginPacket.getStaticCode(), LoginPacket.class);
		packetCodes.put(LoginInfoPacket.getStaticCode(), LoginInfoPacket.class);
		packetCodes.put(CompanyCountPacket.getStaticCode(), CompanyCountPacket.class);
		packetCodes.put(CompanyCountInfoPacket.getStaticCode(), CompanyCountInfoPacket.class);
		packetCodes.put(CityCountPacket.getStaticCode(), CityCountPacket.class);
		packetCodes.put(CityCountInfoPacket.getStaticCode(), CityCountInfoPacket.class);
		packetCodes.put(CourierCountPacket.getStaticCode(), CourierCountPacket.class);
		packetCodes.put(CourierCountInfoPacket.getStaticCode(), CourierCountInfoPacket.class);
		packetCodes.put(ShipCountPacket.getStaticCode(), ShipCountPacket.class);
		packetCodes.put(ShipCountInfoPacket.getStaticCode(), ShipCountInfoPacket.class);

	}
	
	public static PacketManager getInstance() {
		return manager;
	}
	
	public Packet receivePacket(int len, byte[] packetBytes) {
		String msg = new String(packetBytes, 0, len);
		int index = msg.indexOf('@');
		if (index == -1) {
			return null;
		}
		int code = Integer.parseInt(msg.substring(0, index));
		String context = msg.substring(index + 1);
		Class<? extends Packet> packetClazz = packetCodes.get(code);
		try {
			Constructor<? extends Packet> constructor = packetClazz.getConstructor(String.class);
			return constructor.newInstance(context);
		} catch (Exception e) {
			//ThrowableHandler.handleThrowable(e);
		}
		return null;
	}
	
}
