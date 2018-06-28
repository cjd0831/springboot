package springboot.hbase.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestController;
import springboot.hbase.entity.ResponseBody;

/**
 * Created by jingdong on 2018-06-28
 **/
@Slf4j
@ControllerAdvice(annotations = RestController.class)
public class GlobalExceptionHandler {
	@ExceptionHandler(ServiceException.class)
	public ResponseEntity requestExceptionHandler(ServiceException e) {
		return ResponseEntity.status(HttpStatus.BAD_REQUEST)
						.body(ResponseBody.builder().code(40000).msg(e.getMessage()).build());
	}

	@ExceptionHandler(InvalidParameterException.class)
	public ResponseEntity parameterExceptionHandler(ServiceException e) {
		return ResponseEntity.status(HttpStatus.BAD_REQUEST)
						.body(ResponseBody.builder().code(40001).msg(e.getMessage()).build());
	}
}
