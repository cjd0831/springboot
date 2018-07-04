package springboot.hbase.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import springboot.hbase.entity.PutInfo;
import springboot.hbase.entity.ResultInfo;
import springboot.hbase.exception.ServiceException;
import springboot.hbase.util.HBaseConnectionPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by jingdong on 2018-06-26
 **/
@Slf4j
@Service
public class HBaseService {

	/**
	 * 查询HBase中所有表
	 *
	 * @return
	 * @throws IOException
	 */
	public List<String> getListTables() throws IOException {
		return Stream.of(getAdmin().listTables()).map(HTableDescriptor::getNameAsString).collect(Collectors.toList());
	}

	/**
	 * 查询HBase表中数据的数量
	 *
	 * @param tableName
	 * @return
	 */
	public long countByTableName(String tableName) throws IOException {
		long count = 0L;
		HBaseConnectionPool.getInstance();
		Connection connection = HBaseConnectionPool.getConnection().getConnection();
		Table table = connection.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		scan.setFilter(new FirstKeyOnlyFilter());
		ResultScanner scanner = table.getScanner(scan);
		for (Result result : scanner) {
			count += result.size();
		}
		return count;
	}

	/**
	 * 查询HBase表中的所有rowkey
	 *
	 * @param tableName
	 * @return
	 */
	public List<String> getRowKeysByTableName(String tableName) throws IOException {
		List<String> list = new ArrayList<>();
		Scan scan = new Scan();
		ResultScanner scanner = getTableByTableName(tableName).getScanner(scan);
		for (Result result : scanner) {
			String rowKey = Bytes.toString(result.getRow());
			list.add(rowKey);
		}
		return list;
	}

	/**
	 * 根据 rowkey 查询（查询单行数据）
	 *
	 * @param tableName
	 * @param rowKey
	 * @return
	 * @throws IOException
	 */
	public List<ResultInfo> getResultByRowKey(String tableName, String rowKey) throws IOException {
		List<ResultInfo> list = new ArrayList<>();
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = getTableByTableName(tableName).get(get);
		for (Cell cell : result.rawCells()) {
			ResultInfo info = ResultInfo.builder()
							.family(Bytes.toString(CellUtil.cloneFamily(cell)))
							.qualifier(Bytes.toString(CellUtil.cloneQualifier(cell)))
							.value(Bytes.toString(CellUtil.cloneValue(cell)))
							.timestamp(cell.getTimestamp())
							.build();
			list.add(info);
		}
		return list;
	}

	/**
	 * 根据 tableName 查询HBase表所有列族
	 *
	 * @return
	 */
	public List<String> getAllFamiliesByTableName(String tableName) throws IOException {
		return getTableByTableName(tableName)
						.getTableDescriptor()
						.getFamilies()
						.stream()
						.map(HColumnDescriptor::getNameAsString)
						.collect(Collectors.toList());
	}

	/**
	 * 根据表名 获得表的描述
	 *
	 * @param tableName
	 * @return
	 */
	public String getDescribeTable(String tableName) throws IOException {
		return getAdmin()
						.getTableDescriptor(TableName.valueOf(tableName))
						.toString();
	}

	/**
	 * 创建新表
	 *
	 * @param tableName
	 * @param familyNames
	 */
	public void createTable(String tableName, String[] familyNames) throws IOException {
		Admin admin = getAdmin();
		boolean b = admin.tableExists(TableName.valueOf(tableName));
		if (b) {
			throw new ServiceException("表已经存在！");
		}

		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		for (String family : familyNames) {
			tableDescriptor.addFamily(new HColumnDescriptor(family));
		}

		admin.createTable(tableDescriptor);
	}

	/**
	 * 删除表
	 *
	 * @param tableName
	 * @throws IOException
	 */
	public void removeTable(String tableName) throws IOException {
		TableName name = TableName.valueOf(tableName);
		Admin admin = getAdmin();
		if (admin.tableExists(name)) {
			admin.disableTable(name);
			admin.deleteTable(name);
		}
	}

	/**
	 * 添加新的列族
	 *
	 * @param tableName
	 * @param familyNames
	 * @throws IOException
	 */
	public void addNewFamily(String tableName, String[] familyNames) throws IOException {
		Admin admin = getAdmin();
		TableName table = TableName.valueOf(tableName);
		boolean b = admin.tableExists(table);
		if (!b) {
			throw new ServiceException("表不存在！");
		}

		admin.disableTable(table);
		for (String family : familyNames) {
			admin.addColumn(table, new HColumnDescriptor(family));
		}
		admin.enableTable(table);
	}

	/**
	 * 删除列族
	 *
	 * @param tableName
	 * @param familyNames
	 * @throws IOException
	 */
	public void removeFamily(String tableName, String[] familyNames) throws IOException {
		Admin admin = getAdmin();
		TableName table = TableName.valueOf(tableName);
		boolean b = admin.tableExists(table);
		if (!b) {
			throw new ServiceException("表不存在！");
		}

		admin.disableTable(table);
		for (String family : familyNames) {
			try {
				admin.deleteColumn(table, Bytes.toBytes(family));
			} catch (InvalidFamilyOperationException e) {
				admin.enableTable(table);
				throw new ServiceException("删除的family: " + family + "不存在");
			}
		}
		admin.enableTable(table);
	}

	/**
	 * HBase表 添加新的数据或修改数据
	 *
	 * @param tableName
	 * @param list
	 * @throws IOException
	 */
	public void addOrUpdateData(String tableName, List<PutInfo> list) throws IOException {
		isExistTable(tableName);

		Table table = getTableByTableName(tableName);
		List<Put> puts = list.stream().map(info -> {
			Put put = new Put(Bytes.toBytes(info.getRowKey()));
			put.addColumn(Bytes.toBytes(info.getFamily()), Bytes.toBytes(info.getQualifier()), Bytes.toBytes(info.getValue()));
			return put;
		}).collect(Collectors.toList());
		table.put(puts);
	}

	/**
	 * 获取某个限定符的 值
	 *
	 * @param tableName
	 * @param rowkey
	 * @param family
	 * @param qualifier
	 * @throws IOException
	 */
	public String getValueByKey(String tableName, String rowkey, String family, String qualifier) throws IOException {
		isExistTable(tableName);
		Table table = getTableByTableName(tableName);
		Get get = new Get(Bytes.toBytes(rowkey));
		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		Result result = table.get(get);
		Cell[] cells = result.rawCells();
		List<String> list = Stream.of(cells).map(cell -> Bytes.toString(CellUtil.cloneValue(cell))).collect(Collectors.toList());
		if (CollectionUtils.isEmpty(list)) {
			return null;
		}
		return list.get(0);
	}

	/**
	 * 扫描全表
	 *
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public Map<String, List<ResultInfo>> scanTable(String tableName) throws IOException {
		isExistTable(tableName);
		Map<String, List<ResultInfo>> map = new HashMap<>();
		Table table = getTableByTableName(tableName);
		Scan scan = new Scan();
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			List<Cell> cells = result.listCells();
			for (Cell cell : cells) {
				ResultInfo info = ResultInfo.builder()
								.family(Bytes.toString(CellUtil.cloneFamily(cell)))
								.qualifier(Bytes.toString(CellUtil.cloneQualifier(cell)))
								.value(Bytes.toString(CellUtil.cloneValue(cell)))
								.timestamp(cell.getTimestamp())
								.build();

				String key = Bytes.toString(CellUtil.cloneRow(cell));
				if (map.containsKey(key)) {
					List<ResultInfo> resultInfos = map.get(key);
					resultInfos.add(info);
				} else {
					List<ResultInfo> resultInfos = new ArrayList<>();
					resultInfos.add(info);
					map.put(key, resultInfos);
				}

			}
		}
		return map;
	}


	private void isExistTable(String tableName) throws IOException {
		Admin admin = getAdmin();
		TableName tableN = TableName.valueOf(tableName);
		boolean b = admin.tableExists(tableN);
		if (!b) {
			throw new ServiceException("表不存在！");
		}
	}


	private Admin getAdmin() throws IOException {
		HBaseConnectionPool.getInstance();
		Connection connection = HBaseConnectionPool.getConnection().getConnection();
		return connection.getAdmin();
	}

	private Table getTableByTableName(String tableName) throws IOException {
		HBaseConnectionPool.getInstance();
		Connection connection = HBaseConnectionPool.getConnection().getConnection();
		return connection.getTable(TableName.valueOf(tableName));
	}

}
