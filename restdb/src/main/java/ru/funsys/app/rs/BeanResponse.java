/**
 * 
 */
package ru.funsys.app.rs;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author Валерий Лиховских
 *
 */
@XmlRootElement
public class BeanResponse {

	private String query;
	
	private int result;

	private long timer;
	
	public BeanResponse() {
		super();
	}

	public BeanResponse(String query, int result) {
		super();
		this.query = query;
		this.result = result;
	}

	public String getQuery() {
		return query;
	}

	public int getResult() {
		return result;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public void setResult(int result) {
		this.result = result;
	}

	public long getTimer() {
		return timer;
	}

	public void setTimer(long timer) {
		this.timer = timer;
	}

}
