package com.richardyao.code.clearmemcache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;

/**
 * @author YaoXiansheng
 * @date 2018年3月30日
 * @title MainInterface
 * @todo TODO
 */

public class MainInterface {

	public static String memcache_host = null;
	public static Integer memcache_port = null;
	public static Long sleepTime = 0L;
	public static Long defaultTimeout = 0L;
	
	public static void main(String[] args) {
		if(args == null) {
			System.out.println("Lost memcache host and post!");
		} else if(args.length != 4) {
			System.out.println("Wrong parameters!");
		} else {
			memcache_host = args[0];
			memcache_port = Integer.parseInt(args[1]);
			sleepTime = Long.parseLong(args[2]);
			defaultTimeout = Long.parseLong(args[3]);
			if(defaultTimeout < 7) {
				defaultTimeout = 7000L;
			} else {
				defaultTimeout = defaultTimeout * 1000L;
			}
			if(sleepTime < 5) {
				sleepTime = 5000L;
			} else {
				sleepTime = sleepTime * 1000L;
			}
		}
		while(true) {
			clearExpiredData();
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void clearExpiredData() {
		long infiniteTime = 0L;
		try {
			InetSocketAddress connect = new InetSocketAddress(memcache_host, memcache_port);
			List<InetSocketAddress> connectionList = new ArrayList<InetSocketAddress>();
			connectionList.add(connect);
			MemcachedClient mc = new MemcachedClient(new DefaultConnectionFactory() {

				@Override
				public long getOperationTimeout() {
					return defaultTimeout;
				}

			}, connectionList);
			// setTestData(mc);
			Map<SocketAddress, Map<String, String>> memcacheStats = mc.getStats();
			if(memcacheStats != null) {
				Map<String, String> itemsData = memcacheStats.values().iterator().next();
				if(itemsData.containsKey("time") && itemsData.containsKey("uptime")) {
					Long time = Long.parseLong(itemsData.get("time"));
					Long uptime = Long.parseLong(itemsData.get("uptime"));
					infiniteTime = time - uptime;
				}
			}
			
			Map<SocketAddress, Map<String, String>> items = mc.getStats("items");
			Map<Integer, Integer> itemsKeyNumMap = new HashMap<Integer, Integer>();
			if(items != null) {
				Map<String, String> itemsData = items.values().iterator().next();
				if(itemsData.size() > 0) {
					for(Entry<String, String> tempItem : itemsData.entrySet()) {
						String key = tempItem.getKey();
						String value = tempItem.getValue();
						if(key.startsWith("items:") && key.endsWith(":number")) {
							int itemOrder = Integer.parseInt(key.split(":")[1]);
							itemsKeyNumMap.put(itemOrder, Integer.parseInt(value));
						}
					}
				}
			}
			List<String> expiredKey = new ArrayList<String>();
			if(itemsKeyNumMap.size() > 0) {
				long currentSeconds = System.currentTimeMillis() / 1000;
				System.out.println("now time: " + currentSeconds);
				for(Entry<Integer, Integer> keyEntry : itemsKeyNumMap.entrySet()) {
					System.out.println("items " + keyEntry.getKey() + " exist key num: " + keyEntry.getValue());
					String cmd = keyEntry.getKey() + " " + keyEntry.getValue();
					Map<SocketAddress, Map<String, String>> tempItems = mc.getStats("cachedump " + cmd);
					Map<String, String> itemsValue = tempItems.values().iterator().next();
					
					for(Entry<String, String> memcacheKey : itemsValue.entrySet()) {
						String key = memcacheKey.getKey();
						String[] value = memcacheKey.getValue().split(" ");
						long expireTime = Long.parseLong(value[2]);
						if(currentSeconds >= expireTime && expireTime != infiniteTime) { // expireTime is not infinited
							expiredKey.add(key);
						}
					}
				}
			}
			if(expiredKey.size() > 0) {
				System.out.println("Expired keys number: " + expiredKey.size());
				List<OperationFuture<Boolean>> executeResult = new ArrayList<OperationFuture<Boolean>>();
				for(String key : expiredKey) {
					executeResult.add(mc.delete(key));
				}
				int successDeleteNum = 0;
				int failDeleteNum = 0;
				for(int i = 0; i < executeResult.size(); i++) {
					try {
						OperationFuture<Boolean> deleteFuture = executeResult.get(i);
						boolean result = deleteFuture.get(); // wait to delete execute
						OperationStatus operationCode = deleteFuture.getStatus();
						if(result) {
							successDeleteNum++;
						} else {
							if(operationCode.getStatusCode().equals(StatusCode.ERR_NOT_FOUND)) {
								successDeleteNum++;
							} else {
								System.out.println("Delete failed! The errorCode:" + deleteFuture.getStatus());
								failDeleteNum++;
							}
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}
				System.out.println("Delete expired key successfully number: " + successDeleteNum + ", failed number: " + failDeleteNum);
			}
			mc.shutdown();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void setTestData(MemcachedClient mc) {
		int exp = 90;
		for(int i = 0; i < 10; i++) {
			String key = UUID.randomUUID().toString();
			String value = UUID.randomUUID().toString();
			System.out.println(key);
			mc.set(key, exp, value);
		}
	}
}
