package springboot.hbase.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Created by jingdong on 2018-06-27
 **/
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ResponseBody {
	private int code;
	private String msg;
	private Map<String, Object> data;
}
