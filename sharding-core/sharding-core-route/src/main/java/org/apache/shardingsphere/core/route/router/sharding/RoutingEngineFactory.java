/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.core.route.router.sharding;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.core.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.core.preprocessor.statement.SQLStatementContext;
import org.apache.shardingsphere.core.parse.sql.statement.SQLStatement;
import org.apache.shardingsphere.core.parse.sql.statement.dal.DALStatement;
import org.apache.shardingsphere.core.parse.sql.statement.dal.dialect.mysql.ShowDatabasesStatement;
import org.apache.shardingsphere.core.parse.sql.statement.dal.dialect.mysql.UseStatement;
import org.apache.shardingsphere.core.parse.sql.statement.dal.dialect.postgresql.ResetParameterStatement;
import org.apache.shardingsphere.core.parse.sql.statement.dal.dialect.postgresql.SetStatement;
import org.apache.shardingsphere.core.parse.sql.statement.dcl.DCLStatement;
import org.apache.shardingsphere.core.parse.sql.statement.ddl.DDLStatement;
import org.apache.shardingsphere.core.parse.sql.statement.dml.DMLStatement;
import org.apache.shardingsphere.core.parse.sql.statement.dml.SelectStatement;
import org.apache.shardingsphere.core.parse.sql.statement.tcl.TCLStatement;
import org.apache.shardingsphere.core.route.router.sharding.condition.ShardingConditions;
import org.apache.shardingsphere.core.route.type.RoutingEngine;
import org.apache.shardingsphere.core.route.type.broadcast.DataSourceGroupBroadcastRoutingEngine;
import org.apache.shardingsphere.core.route.type.broadcast.DatabaseBroadcastRoutingEngine;
import org.apache.shardingsphere.core.route.type.broadcast.MasterInstanceBroadcastRoutingEngine;
import org.apache.shardingsphere.core.route.type.broadcast.TableBroadcastRoutingEngine;
import org.apache.shardingsphere.core.route.type.complex.ComplexRoutingEngine;
import org.apache.shardingsphere.core.route.type.defaultdb.DefaultDatabaseRoutingEngine;
import org.apache.shardingsphere.core.route.type.ignore.IgnoreRoutingEngine;
import org.apache.shardingsphere.core.route.type.standard.StandardRoutingEngine;
import org.apache.shardingsphere.core.route.type.unicast.UnicastRoutingEngine;
import org.apache.shardingsphere.core.rule.ShardingRule;

import java.util.Collection;

/**
 * Routing engine factory.
 *
 * @author zhangliang
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RoutingEngineFactory {
    
    /**
     * Create new instance of routing engine.
     * 所有类型 RoutingEngine 的创建入口
     * @param shardingRule sharding rule
     * @param metaData meta data of ShardingSphere
     * @param sqlStatementContext SQL statement context
     * @param shardingConditions shardingConditions
     * @return new instance of routing engine
     */
    public static RoutingEngine newInstance(final ShardingRule shardingRule,
                                            final ShardingSphereMetaData metaData, final SQLStatementContext sqlStatementContext, final ShardingConditions shardingConditions) {
        //SQLStatement的生成是sql解析引擎职责，我们当前略过，后面会分析解析引擎的逻辑
        SQLStatement sqlStatement = sqlStatementContext.getSqlStatement();
        // 逻辑表名
        Collection<String> tableNames = sqlStatementContext.getTablesContext().getTableNames();
        if (sqlStatement instanceof TCLStatement) {
            return new DatabaseBroadcastRoutingEngine(shardingRule);
        }
        if (sqlStatement instanceof DDLStatement) {
            return new TableBroadcastRoutingEngine(shardingRule, metaData.getTables(), sqlStatementContext);
        }
        if (sqlStatement instanceof DALStatement) {
            return getDALRoutingEngine(shardingRule, sqlStatement, tableNames);
        }
        if (sqlStatement instanceof DCLStatement) {
            return getDCLRoutingEngine(shardingRule, sqlStatementContext, metaData);
        }
        if (shardingRule.isAllInDefaultDataSource(tableNames)) {
            return new DefaultDatabaseRoutingEngine(shardingRule, tableNames);
        }
        if (shardingRule.isAllBroadcastTables(tableNames)) {
            return sqlStatement instanceof SelectStatement ? new UnicastRoutingEngine(shardingRule, tableNames) : new DatabaseBroadcastRoutingEngine(shardingRule);
        }
        if (sqlStatementContext.getSqlStatement() instanceof DMLStatement && tableNames.isEmpty() && shardingRule.hasDefaultDataSourceName()) {
            return new DefaultDatabaseRoutingEngine(shardingRule, tableNames);
        }
        if (sqlStatementContext.getSqlStatement() instanceof DMLStatement && shardingConditions.isAlwaysFalse() || tableNames.isEmpty()) {
            return new UnicastRoutingEngine(shardingRule, tableNames);
        }
        //这里是生成标准路由引擎的入口
        return getShardingRoutingEngine(shardingRule, sqlStatementContext, shardingConditions, tableNames);
    }
    
    private static RoutingEngine getDALRoutingEngine(final ShardingRule shardingRule, final SQLStatement sqlStatement, final Collection<String> tableNames) {
        if (sqlStatement instanceof UseStatement) {
            return new IgnoreRoutingEngine();
        }
        if (sqlStatement instanceof SetStatement || sqlStatement instanceof ResetParameterStatement || sqlStatement instanceof ShowDatabasesStatement) {
            return new DatabaseBroadcastRoutingEngine(shardingRule);
        }
        if (!tableNames.isEmpty()) {
            return new UnicastRoutingEngine(shardingRule, tableNames);
        }
        return new DataSourceGroupBroadcastRoutingEngine(shardingRule);
    }
    
    private static RoutingEngine getDCLRoutingEngine(final ShardingRule shardingRule, final SQLStatementContext sqlStatementContext, final ShardingSphereMetaData metaData) {
        return isGrantForSingleTable(sqlStatementContext) 
                ? new TableBroadcastRoutingEngine(shardingRule, metaData.getTables(), sqlStatementContext) : new MasterInstanceBroadcastRoutingEngine(shardingRule, metaData.getDataSources());
    }
    
    private static boolean isGrantForSingleTable(final SQLStatementContext sqlStatementContext) {
        return !sqlStatementContext.getTablesContext().isEmpty() && !"*".equals(sqlStatementContext.getTablesContext().getSingleTableName());
    }
    
    private static RoutingEngine getShardingRoutingEngine(final ShardingRule shardingRule, final SQLStatementContext sqlStatementContext, 
                                                          final ShardingConditions shardingConditions, final Collection<String> tableNames) {
        Collection<String> shardingTableNames = shardingRule.getShardingLogicTableNames(tableNames);
        //对照着看官方文档上“路由引擎的整体结构”图，当满足单表或者全部是绑定表时，走标准路由引擎
        if (1 == shardingTableNames.size() || shardingRule.isAllBindingTables(shardingTableNames)) {
            //准路由引擎，此处可以知道入参分别是分片规则、分片逻辑表、SQLStatement上下文、分片条件
            //注意，由于还没有开始进行分片算法运算，此时的shardingConditions只有用于分片计算的RouteValue，没有分片结果DataNode
            return new StandardRoutingEngine(shardingRule, shardingTableNames.iterator().next(), sqlStatementContext, shardingConditions);
        }
        // TODO config for cartesian set
        // 混合路由引擎，此处暂不分析
        return new ComplexRoutingEngine(shardingRule, tableNames, sqlStatementContext, shardingConditions);
    }
}
