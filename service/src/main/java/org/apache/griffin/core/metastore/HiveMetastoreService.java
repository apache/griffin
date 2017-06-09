/*-
 * Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

 */

package org.apache.griffin.core.metastore;

import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;

public interface HiveMetastoreService {

    public Iterable<String> getAllDatabases();

    public Iterable<String> getAllTableNames(String dbName);

    public List<Table> getAllTable(String db);

    public Map<String,List<Table>> getAllTable();

    public Table getTable(String dbName, String tableName);

}
