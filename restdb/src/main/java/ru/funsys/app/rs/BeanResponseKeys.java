/**
 * 
 */
package ru.funsys.app.rs;

import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author Валерий Лиховских
 *
 */
@XmlRootElement
public class BeanResponseKeys extends BeanResponse {

	private ArrayList<HashMap<String,Object>> keys;

	public BeanResponseKeys() {
		super();
	}

	public BeanResponseKeys(String query, int result) {
		super(query, result);
	}

	public ArrayList<HashMap<String, Object>> getKeys() {
		return keys;
	}

	public void setKeys(ArrayList<HashMap<String, Object>> keys) {
		this.keys = keys;
	}

}
