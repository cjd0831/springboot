package springboot.hbase.exception;

/**
 * Created by jingdong on 2018-06-28
 **/
public class ServiceException extends RuntimeException {
	public ServiceException(String msg) {
		super(msg);
	}
}
