package springboot.hbase.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import springboot.hbase.entity.PutInfo;
import springboot.hbase.entity.ResponseBody;
import springboot.hbase.service.HBaseService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Created by jingdong on 2018-06-26
 **/
@Api(value = "HBase操作相关API", tags = "HBase")
@RestController
public class HBaseOperationController {

	@Autowired
	private HBaseService hBaseService;

	@ApiOperation(tags = "HBase", value = "获取HBase中所有表")
	@GetMapping("/getAllTables")
	public ResponseEntity getListTables() throws IOException {
		return ResponseEntity.ok(ResponseBody
						.builder()
						.data(buildDataMap("tables", hBaseService.getListTables()))
						.code(200)
						.msg("success")
						.build());
	}

	@ApiOperation(tags = "HBase", value = "获取对应HBase表数据的条数")
	@GetMapping("/getCount/{tableName}")
	public ResponseEntity countByTableName(@PathVariable(value = "tableName") String tableName) throws IOException {
		return ResponseEntity.ok(ResponseBody
						.builder()
						.data(buildDataMap("count", hBaseService.countByTableName(tableName)))
						.code(200)
						.msg("success")
						.build());
	}

	@ApiOperation(tags = "HBase", value = "获取对应HBase表所有rowKey")
	@GetMapping("/getRowKeys/{tableName}")
	public ResponseEntity getRowKeysByTableName(@PathVariable(value = "tableName") String tableName) throws IOException {
		return ResponseEntity.ok(ResponseBody
						.builder()
						.data(buildDataMap("rowkeys", hBaseService.getRowKeysByTableName(tableName)))
						.code(200)
						.msg("success")
						.build());
	}

	@ApiOperation(tags = "HBase", value = "根据rowkey查询详细信息")
	@GetMapping("/getResult/{tableName}")
	public ResponseEntity getResultByRowKey(@PathVariable(value = "tableName") String tableName, @RequestParam(value = "rowkey") String rowkey) throws IOException {
		return ResponseEntity.ok(ResponseBody
						.builder()
						.data(buildDataMap("result", hBaseService.getResultByRowKey(tableName, rowkey)))
						.code(200)
						.msg("success")
						.build());
	}

	@ApiOperation(tags = "HBase", value = "获取对应HBase表所有列族")
	@GetMapping("/getAllFamilies/{tableName}")
	public ResponseEntity getAllFamiliesByTableName(@PathVariable(value = "tableName") String tableName) throws IOException {
		return ResponseEntity.ok(ResponseBody
						.builder()
						.data(buildDataMap("familyNames", hBaseService.getAllFamiliesByTableName(tableName)))
						.code(200)
						.msg("success")
						.build());
	}

	@ApiOperation(tags = "HBase", value = "获取对应HBase表的描述")
	@GetMapping("/getTableDescription/{tableName}")
	public ResponseEntity getTableDescriptionByTableName(@PathVariable(value = "tableName") String tableName) throws IOException {
		return ResponseEntity.ok(ResponseBody
						.builder()
						.data(buildDataMap("tableDescription", hBaseService.getDescribeTable(tableName)))
						.code(200)
						.msg("success")
						.build());
	}

	@ApiOperation(tags = "HBase", value = "创建新的HBase表")
	@PostMapping("/createTable/{tableName}")
	public ResponseEntity createTable(@PathVariable(value = "tableName") String tableName, @RequestBody String[] familyNames) throws IOException {
		hBaseService.createTable(tableName, familyNames);
		return ResponseEntity.ok(ResponseBody
						.builder()
						.code(200)
						.msg("success")
						.build());
	}

	@ApiOperation(tags = "HBase", value = "删除HBase表")
	@DeleteMapping("/removeTable/{tableName}")
	public ResponseEntity removeTable(@PathVariable(value = "tableName") String tableName) throws IOException {
		hBaseService.removeTable(tableName);
		return ResponseEntity.ok(ResponseBody
						.builder()
						.code(200)
						.msg("success")
						.build());
	}

	@ApiOperation(tags = "HBase", value = "添加新的family")
	@PutMapping("/addNewFamily/{tableName}")
	public ResponseEntity addNewFamily(@PathVariable(value = "tableName") String tableName, @RequestBody String[] familyNames) throws IOException {
		hBaseService.addNewFamily(tableName, familyNames);
		return ResponseEntity.ok(ResponseBody
						.builder()
						.code(200)
						.msg("success")
						.build());
	}

	@ApiOperation(tags = "HBase", value = "删除family")
	@DeleteMapping("/removeFamily/{tableName}")
	public ResponseEntity removeFamily(@PathVariable(value = "tableName") String tableName, @RequestBody String[] familyNames) throws IOException {
		hBaseService.removeFamily(tableName, familyNames);
		return ResponseEntity.ok(ResponseBody
						.builder()
						.code(200)
						.msg("success")
						.build());
	}


	@ApiOperation(tags = "HBase", value = "获取某个特定限定符的值")
	@GetMapping("/getValue/{tableName}/{rowkey}/{family}/{qualify}")
	public ResponseEntity<ResponseBody> getValue(@PathVariable("tableName") String tableName,
																							 @PathVariable("rowkey") String rowkey,
																							 @PathVariable("family") String family,
																							 @PathVariable("qualify") String qualify) throws IOException {
		return ResponseEntity.ok(ResponseBody
						.builder()
						.code(200)
						.data(buildDataMap("value", hBaseService.getValueByKey(tableName, rowkey, family, qualify)))
						.msg("success")
						.build());

	}

	@ApiOperation(tags = "HBase", value = "添加或修改数据")
	@PutMapping("/addOrUpdateData/{tableName}")
	public ResponseEntity<ResponseBody> addOrUpdateData(@PathVariable("tableName") String tableName, @RequestBody List<PutInfo> puts) throws IOException {
		hBaseService.addOrUpdateData(tableName, puts);
		return ResponseEntity.ok(ResponseBody
						.builder()
						.code(200)
						.msg("success")
						.build());
	}

	@ApiOperation(tags = "HBase", value = "扫描全表")
	@GetMapping("/scanTable/{tableName}")
	public ResponseEntity<ResponseBody> scanTable(@PathVariable("tableName") String tableName) throws IOException {
		return ResponseEntity.ok(ResponseBody
						.builder()
						.code(200)
						.data(buildDataMap("values", hBaseService.scanTable(tableName)))
						.msg("success")
						.build());
	}

	@ApiOperation(tags = "HBase", value = "删除行数据")
	@DeleteMapping("/removeRow/{tableName}")
	public ResponseEntity removeRow(@PathVariable(value = "tableName") String tableName, @RequestBody List<String> rowKeys) throws IOException {
		hBaseService.removeRow(tableName, rowKeys);
		return ResponseEntity.ok(ResponseBody
						.builder()
						.code(200)
						.msg("success")
						.build());
	}

	private Map<String, Object> buildDataMap(String key, Object value) {
		Map<String, Object> data = new HashMap<>();
		if (!Objects.isNull(value)) {
			data.put(key, value);
		}
		return data;
	}
}
