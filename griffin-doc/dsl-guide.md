<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Apache Griffin DSL Guide
Griffin DSL is designed for DQ measurement, as a SQL-like language, trying to describe the DQ domain request.

## Griffin DSL Syntax Description
Griffin DSL is SQL-like, case insensitive, and easy to learn.

### Supporting process
- logical operation: not, and, or, in, between, like, is null, is nan, =, !=, <=, >=, <, >
- mathematical operation: +, -, *, /, %
- sql statement: as, where, group by, having, order by, limit


### Keywords
- `null, nan, true, false`
- `not, and, or`
- `in, between, like, is`
- `as, where, group, by, having, order, desc, asc, limit`

### Operators
- `!, &&, ||, =, !=, <, >, <=, >=, <>`
- `+, -, *, /, %`
- `(, )`
- `., [, ]`

### Literals
- **string**: any string surrounded with a pair of " or ', with escape charactor \ if any request.  
	e.g. `"test", 'string 1', "hello \" world \" "`
- **number**: double or integer number.  
	e.g. `123, 33.5`
- **time**: a integer with unit in a string, will be translated to a integer number in millisecond.  
	e.g. `3d, 5h, 4ms`
- **boolean**: boolean value directly.  
	e.g. `true, false`

### Selections
- **selection head**: data source name.  
	e.g. `source, target`
- **all field selection**: * or with data source name ahead.  
	e.g. `*, source.*, target.*`
- **field selection**: field name or with data source name ahead.  
	e.g. `source.age, target.name, user_id`
- **index selection**: interget between square brackets "[]" with field name ahead.  
	e.g. `source.attributes[3]`
- **function selection**: function name with brackets "()", with field name ahead or not.  
	e.g. `count(*), *.count(), source.user_id.count(), max(source.age)`
- **alias**: declare an alias after a selection.  
	e.g. `source.user_id as id, target.user_name as name`

### Math expressions
- **math factor**: literal or function or selection or math exression with brackets.  
	e.g. `123, max(1, 2, 3, 4), source.age, (source.age + 13)`
- **unary math expression**: unary math operator with factor.  
	e.g. `-(100 - source.score)`
- **binary math expression**: math factors with binary math operators.  
	e.g. `source.age + 13, score * 2 + ratio`

### Logical expression
- **in**: in clause like sql.  
	e.g. `source.country in ("USA", "CHN", "RSA")`
- **between**: between clause like sql.  
	e.g. `source.age between 3 and 30, source.age between (3, 30)`
- **like**: like clause like sql.  
	e.g. `source.name like "%abc%"`
- **is null**: is null operator like sql.  
	e.g. `source.desc is not null`
- **is nan**: check if the value is not a number, the syntax like `is null`  
	e.g. `source.age is not nan`
- **logical factor**: math expression or logical expressions above or other logical expressions with brackets.  
	e.g. `(source.user_id = target.user_id AND source.age > target.age)`
- **unary logical expression**: unary logical operator with factor.  
	e.g. `NOT source.has_data`
- **binary logical expression**: logical factors with binary logical operators, including `and`, `or` and comparison operators.  
	e.g. `source.age = target.age OR source.ticket = target.tck`


### Expression
- **expression**: logical expression and math expression.

### Function
- **argument**: expression.
- **function**: function name with arguments between brackets.  
	e.g. `max(source.age, target.age), count(*)`

### Clause
- **select clause**: 