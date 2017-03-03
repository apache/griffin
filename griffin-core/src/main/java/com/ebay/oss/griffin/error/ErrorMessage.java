/*
	Copyright (c) 2016 eBay Software Foundation.
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	    http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
 */
package com.ebay.oss.griffin.error;

import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ErrorMessage {
	/** contains the same HTTP Status code returned by the server */
	@XmlElement(name = "status")
	int status;

	/** application specific error code */
	@XmlElement(name = "code")
	int code;

	/** message describing the error */
	@XmlElement(name = "message")
	String message;

	/** link point to page where the error message is documented */
	@XmlElement(name = "link")
	String link;

	/** extra information that might useful for developers */
	@XmlElement(name = "developerMessage")
	String developerMessage;

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getDeveloperMessage() {
		return developerMessage;
	}

	public void setDeveloperMessage(String developerMessage) {
		this.developerMessage = developerMessage;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	public ErrorMessage(Throwable ex) {
		this.status = Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();
		this.message = ex.getMessage();

	}

	public ErrorMessage() {
	}

	public ErrorMessage(String msg) {
		this.message = msg;
	}

	public ErrorMessage(int status, String msg) {
		this.status = status;
		this.message = msg;
	}

}
