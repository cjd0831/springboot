package springboot.hbase.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by jingdong on 2018-06-27
 **/
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ResultInfo implements Serializable {
	private String family;
	private String qualifier;
	private String value;
	private Long timestamp;
}
