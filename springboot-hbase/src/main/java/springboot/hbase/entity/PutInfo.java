package springboot.hbase.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Created by jingdong on 2018-06-28
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PutInfo implements Serializable {
	private String rowKey;
	private String family;
	private String qualifier;
	private String value;
}
