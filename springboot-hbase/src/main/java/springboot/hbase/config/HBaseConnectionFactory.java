package springboot.hbase.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Created by jingdong on 2018-06-26
 **/
@Slf4j
@Component
public class HBaseConnectionFactory implements InitializingBean {

	@Value("${hbase.zookeeper.quorum}")
	private String zkQuorum;

	@Value("${hbase.master}")
	private String hBaseMaster;

	@Value("${hbase.zookeeper.property.clientPort}")
	private String zkPort;

	@Value("${zookeeper.znode.parent}")
	private String znode;

	private static Configuration conf = HBaseConfiguration.create();

	public static Connection connection;

	@Override
	public void afterPropertiesSet() {
		conf.set("hbase.zookeeper.quorum", zkQuorum);
		conf.set("hbase.zookeeper.property.clientPort", zkPort);
		conf.set("zookeeper.znode.parent", znode);
		conf.set("hbase.master", hBaseMaster);
		try {
			connection = ConnectionFactory.createConnection(conf);
			log.info("获取connection连接成功！");
		} catch (IOException e) {
			e.printStackTrace();
			log.error("获取connection连接失败！");
		}
	}

}
