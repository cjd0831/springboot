package springboot.hbase.exception;

/**
 * Created by jingdong on 2018-06-28
 **/
public class InvalidParameterException extends RuntimeException {
	public InvalidParameterException(String msg) {
		super(msg);
	}
}
