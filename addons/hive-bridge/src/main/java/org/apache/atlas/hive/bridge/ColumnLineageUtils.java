/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.hive.bridge;

import org.apache.atlas.hive.model.HiveDataModelGenerator;
import org.apache.atlas.hive.model.HiveDataTypes;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ColumnLineageUtils {

    public static class ColumnName {
        public final String dbName;
        public final String tableName;
        public final String colName;

        public ColumnName(LineageInfo.BaseColumnInfo col) {
            dbName = col.getTabAlias().getTable().getDbName();
            tableName = col.getTabAlias().getTable().getTableName();
            colName = col.getColumn().getName();
        }

        public ColumnName(LineageInfo.DependencyKey depKey) {
            dbName = depKey.getDataContainer().getTable().getDbName();
            tableName = depKey.getDataContainer().getTable().getTableName();
            colName = depKey.getFieldSchema().getName();
        }

        public ColumnName(String[] qName, String colName) {
            dbName = qName.length > 1 ? qName[0] : "" /* this shouldn't happen */;
            tableName = qName.length > 1 ? qName[1] : qName[0] /* this shouldn't happen */;
            this.colName = colName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ColumnName that = (ColumnName) o;

            if (dbName != null ? !dbName.equals(that.dbName) : that.dbName != null) return false;
            if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null) return false;
            return colName != null ? colName.equals(that.colName) : that.colName == null;

        }

        @Override
        public int hashCode() {
            int result = dbName != null ? dbName.hashCode() : 0;
            result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
            result = 31 * result + (colName != null ? colName.hashCode() : 0);
            return result;
        }
    }

    public static class HiveColumnLineageInfo {
        public final String depenendencyType;
        public final String expr;
        public final ColumnName inputColumn;

        HiveColumnLineageInfo(LineageInfo.Dependency d, LineageInfo.BaseColumnInfo inputCol) {
            depenendencyType = d.getType().name();
            expr = d.getExpr();
            inputColumn = new ColumnName(inputCol);
        }
    }

    public static Map<ColumnName, List<HiveColumnLineageInfo>> buildLineageMap(LineageInfo lInfo) {
        Map<ColumnName, List<HiveColumnLineageInfo>> m = new HashMap<>();

        for(Map.Entry<LineageInfo.DependencyKey, LineageInfo.Dependency> e : lInfo.entrySet()) {
            List<HiveColumnLineageInfo> l = new ArrayList<>();
            ColumnName k = new ColumnName(e.getKey());
            for(LineageInfo.BaseColumnInfo iCol : e.getValue().getBaseCols()) {
                l.add(new HiveColumnLineageInfo(e.getValue(), iCol));
            }
            m.put(k, l);
        }
        return m;
    }

    static String[] extractComponents(String qualifiedName) {
        String[] comps = qualifiedName.split("\\.");
        int lastIdx = comps.length - 1;
        int atLoc = comps[lastIdx].indexOf('@');
        if (atLoc > 0 ) {
            comps[lastIdx] = comps[lastIdx].substring(0, atLoc);
        }
        return comps;
    }

    static void populateColumnReferenceableMap(Map<ColumnName, Referenceable> m,
                                               Referenceable r) {
        if (r.getTypeName().equals(HiveDataTypes.HIVE_TABLE.getName())) {
            String qName = (String) r.get(HiveDataModelGenerator.NAME);
            String[] qNameComps = extractComponents(qName);
            for(Referenceable col : (List<Referenceable>) r.getValuesMap().get("columns") ) {
                String cName = (String) col.get(HiveDataModelGenerator.COLUMN_NAME);
                m.put(new ColumnName(qNameComps, cName), col);
            }
        }
    }


    public static Map<ColumnName, Referenceable> buildColumnReferenceableMap(List<Referenceable> inputs,
                                                                             List<Referenceable> outputs) {
        Map<ColumnName, Referenceable> m = new HashMap<>();

        for(Referenceable r : inputs) {
            populateColumnReferenceableMap(m, r);
        }

        for(Referenceable r : outputs) {
            populateColumnReferenceableMap(m, r);
        }

        return m;
    }

}
