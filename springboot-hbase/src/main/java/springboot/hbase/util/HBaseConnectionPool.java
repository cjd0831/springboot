package springboot.hbase.util;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * HBase Connection Pool
 * <p>
 * Created by jingdong on 2018-07-04
 **/
public class HBaseConnectionPool {
	private static ConcurrentHashMap<String, HBaseConnectionEntity> idleConnections = null;  //空闲连接
	private static ConcurrentHashMap<String, HBaseConnectionEntity> activeConnections = null;  //活跃连接
	private static int initSize;  //初始化连接数
	private static int maxSize;    //连接池中最大连接数
	private static AtomicInteger idleSize = new AtomicInteger(0);  //空闲连接数
	private static AtomicInteger activeSize = new AtomicInteger(0);  //活跃连接数
	private static HBaseConnectionPool instance = null;
	private static Lock lock = new ReentrantLock();
	private static volatile boolean isShutdown = false;

	private HBaseConnectionPool(int initSize, int maxSize) {
		HBaseConnectionPool.initSize = initSize;
		HBaseConnectionPool.maxSize = maxSize;
		idleConnections = new ConcurrentHashMap<>();
		activeConnections = new ConcurrentHashMap<>();
		initConnections();
		new HBaseDetectFailConnection().start();
	}

	/**
	 * 初始化连接池
	 */
	private void initConnections() {
		for (int i = 0; i < HBaseConnectionPool.initSize; i++) {
			HBaseConnectionEntity entity = new HBaseConnectionEntity();
			String id = UUID.randomUUID().toString();
			entity.setId(id);
			Connection conn = HBaseConnectionFactory.getConnection();
			if (conn == null) {
				continue;
			}
			entity.setConnection(conn);
			entity.setStatus(HBaseConnectionStatus.idle);
			idleConnections.put(id, entity);
			idleSize.getAndAdd(1);
		}
	}

	/**
	 * 从连接池获取连接
	 *
	 * @return HBase连接实体
	 */
	public static HBaseConnectionEntity getConnection() {
		if (isShutdown) {
			throw new RuntimeException("pool is shutdown.");
		}
		lock.lock();
		try {
			if (idleSize.get() > 0) {
				if (idleConnections.size() <= 0) {
					throw new RuntimeException("当前没有空闲连接");
				}
				Map.Entry<String, HBaseConnectionEntity> entry = idleConnections.entrySet().iterator().next();
				String key = entry.getKey();
				HBaseConnectionEntity entity = entry.getValue();
				entity.setStatus(HBaseConnectionStatus.active);
				idleConnections.remove(key);
				idleSize.decrementAndGet();
				if (entity.getConnection().isClosed()) {
					return getConnection();
				}
				activeConnections.put(key, entity);
				activeSize.incrementAndGet();
				return entity;
			}
		} finally {
			lock.unlock();
		}

		if (activeSize.get() > maxSize) {
			throw new RuntimeException("活跃数量大于最大值");
		}
		if (activeSize.get() <= maxSize) {
			synchronized (HBaseConnectionPool.class) {
				try {
					HBaseConnectionPool.class.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			return getConnection();
		}

		if (isShutdown) {
			throw new RuntimeException("pool is shutdown.");
		}

		Connection conn = HBaseConnectionFactory.getConnection();
		String id = UUID.randomUUID().toString();
		HBaseConnectionEntity entity = new HBaseConnectionEntity();
		entity.setId(id);
		entity.setConnection(conn);
		entity.setStatus(HBaseConnectionStatus.active);
		activeConnections.put(id, entity);
		activeSize.incrementAndGet();
		return entity;
	}

	/**
	 * 初始化HBase连接池
	 *
	 * @return
	 */
	public static HBaseConnectionPool getInstance() {
		if (isShutdown) {
			throw new RuntimeException("pool is already shutdown.");
		}
		if (instance != null) {
			return instance;
		}
		return getInstance(20, 20);
	}

	public static HBaseConnectionPool getInstance(int initSize, int maxSize) {
		if (isShutdown) {
			throw new RuntimeException("pool is already shutdown.");
		}
		if (initSize < 0 || maxSize < 1) {
			throw new RuntimeException("initSize必须不小于0，maxsize必须大于等于1");
		}
		if (initSize > maxSize) {
			initSize = maxSize;
		}
		synchronized (HBaseConnectionPool.class) {
			if (instance == null) {
				instance = new HBaseConnectionPool(initSize, maxSize);
			}
		}
		return instance;
	}

	/**
	 * 释放连接
	 *
	 * @param id
	 */
	public void releaseConnection(String id) {
		if (isShutdown) {
			throw new RuntimeException("pool is shutdown.");
		}
		if (idleSize.get() == maxSize) {
			HBaseConnectionFactory.closeConnection(activeConnections.remove(id).getConnection());
		} else {
			HBaseConnectionEntity entity = activeConnections.remove(id);
			entity.setStatus(HBaseConnectionStatus.idle);
			idleConnections.put(id, entity);
			idleSize.incrementAndGet();
			activeSize.decrementAndGet();
			synchronized (HBaseConnectionPool.class) {
				HBaseConnectionPool.class.notify();
			}

		}
	}

	/**
	 * 关闭连接池
	 */
	public void shutdown() {
		isShutdown = true;
		synchronized (HBaseConnectionPool.class) {
			HBaseConnectionPool.class.notifyAll();
		}
		for (String key : idleConnections.keySet()) {
			HBaseConnectionEntity entity = idleConnections.get(key);
			HBaseConnectionFactory.closeConnection(entity.getConnection());
		}
		for (String key : activeConnections.keySet()) {
			HBaseConnectionEntity entity = activeConnections.get(key);
			HBaseConnectionFactory.closeConnection(entity.getConnection());
		}
		initSize = 0;
		maxSize = 0;
		idleSize = new AtomicInteger(0);
		activeSize = new AtomicInteger(0);
	}

	public int getidleSize() {
		return HBaseConnectionPool.idleSize.get();
	}

	public int getActiveSize() {
		return HBaseConnectionPool.activeSize.get();
	}

	class HBaseDetectFailConnection extends Thread {
		@Override
		public void run() {
			Iterator<String> itIdle = idleConnections.keySet().iterator();
			handleConnections(itIdle, idleConnections, idleSize);

			Iterator<String> itActive = activeConnections.keySet().iterator();
			handleConnections(itActive, activeConnections, activeSize);
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void handleConnections(Iterator<String> itIdle, ConcurrentHashMap<String, HBaseConnectionEntity> idleConnections, AtomicInteger idleSize) {
		while (itIdle.hasNext()) {
			String key = itIdle.next();
			HBaseConnectionEntity entity = idleConnections.get(key);
			if (entity.getConnection().isClosed()) {
				idleConnections.remove(key);
				idleSize.decrementAndGet();
			}
		}
	}

	@Slf4j
	@Component
	public static class HBaseConnectionFactory implements InitializingBean {
		@Value("${hbase.zookeeper.quorum}")
		private String zkQuorum;

		@Value("${hbase.master}")
		private String hBaseMaster;

		@Value("${hbase.zookeeper.property.clientPort}")
		private String zkPort;

		@Value("${zookeeper.znode.parent}")
		private String znode;
		private static Configuration conf = HBaseConfiguration.create();
		private static ExecutorService poolx = Executors.newFixedThreadPool(30);

		private static Connection getConnection() {
			int i = 0;
			Connection conn = null;
			do {
				try {
					conn = ConnectionFactory.createConnection(conf, poolx);
					if (conn != null) {
						break;
					}
					Thread.sleep(100);
					i++;
				} catch (InterruptedException | IOException e) {
					e.printStackTrace();
				}
			} while (i < 5);
			return conn;
		}

		private static void closeConnection(Connection connection) {
			try {
				connection.close();
				poolx.shutdownNow();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void afterPropertiesSet() throws Exception {
			conf.set("hbase.zookeeper.quorum", zkQuorum);
			conf.set("hbase.zookeeper.property.clientPort", zkPort);
			conf.set("zookeeper.znode.parent", znode);
			conf.set("hbase.master", hBaseMaster);

			log.info("加载hbase配置success!");
		}
	}

	enum HBaseConnectionStatus {
		idle,  //空闲
		active,  //活跃（使用中）
		close  //关闭
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HBaseConnectionEntity {
		private String id;
		private Connection connection;
		private HBaseConnectionStatus status;
	}
}