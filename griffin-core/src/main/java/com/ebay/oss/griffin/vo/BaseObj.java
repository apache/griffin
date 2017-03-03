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
package com.ebay.oss.griffin.vo;

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import com.ebay.oss.griffin.error.ErrorMessage;

public class BaseObj {

	public ErrorMessage validate() {
		ErrorMessage msg = null;

		ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
		Validator validator = factory.getValidator();
		Set<ConstraintViolation<BaseObj>> constraintViolations = validator
				.validate(this);

		if (constraintViolations.size() > 0) {
			msg = new ErrorMessage();
			StringBuffer error = new StringBuffer();
			for (ConstraintViolation<BaseObj> violation : constraintViolations) {
				error.append(violation.getPropertyPath() + " " + violation.getMessage() + ", ");
			}

			msg.setMessage(error.toString());

		}

		return msg;

	}
}
