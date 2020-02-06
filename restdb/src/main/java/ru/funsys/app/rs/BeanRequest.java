/**
 * 
 */
package ru.funsys.app.rs;


import javax.ws.rs.DefaultValue;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;


/**
 * @author Валерий Лиховских
 *
 */
public class BeanRequest {

	private MultivaluedMap<String, String> queryParams;
	
    @PathParam("object")
	private String object;

	@HeaderParam("Content-Language") @DefaultValue("ru")
	private String lang;
	  
	@Context
	public void setQueryParameters(UriInfo ui) {
		queryParams = ui.getQueryParameters();
	}

	public MultivaluedMap<String, String> getQueryParams() {
		return queryParams;
	}

	public String getObject() {
		return object;
	}

	public String getLang() {
		return lang;
	}

}
