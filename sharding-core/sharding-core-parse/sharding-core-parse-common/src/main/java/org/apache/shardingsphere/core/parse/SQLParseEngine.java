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

package org.apache.shardingsphere.core.parse;

import com.google.common.base.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.core.parse.cache.SQLParseResultCache;
import org.apache.shardingsphere.core.parse.core.SQLParseKernel;
import org.apache.shardingsphere.core.parse.core.rule.registry.ParseRuleRegistry;
import org.apache.shardingsphere.core.parse.hook.ParsingHook;
import org.apache.shardingsphere.core.parse.hook.SPIParsingHook;
import org.apache.shardingsphere.core.parse.sql.statement.SQLStatement;
import org.apache.shardingsphere.spi.database.DatabaseType;

/**
 * SQL parse engine.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class SQLParseEngine {
    
    private final DatabaseType databaseType;
    
    private final SQLParseResultCache cache = new SQLParseResultCache();
    
    /**
     * Parse SQL.
     *
     * @param sql SQL
     * @param useCache use cache or not
     * @return SQL statement
     */
    public SQLStatement parse(final String sql, final boolean useCache) {
        //内部实现钩子方法，植入其他逻辑，主要作用是链路监控
        ParsingHook parsingHook = new SPIParsingHook();
        parsingHook.start(sql);
        try {
            //解析入口
            SQLStatement result = parse0(sql, useCache);
            parsingHook.finishSuccess(result);
            return result;
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            parsingHook.finishFailure(ex);
            throw ex;
        }
    }
    
    private SQLStatement parse0(final String sql, final boolean useCache) {
        //根据是否使用缓存获取解析结果
        if (useCache) {
            Optional<SQLStatement> cachedSQLStatement = cache.getSQLStatement(sql);
            if (cachedSQLStatement.isPresent()) {
                return cachedSQLStatement.get();
            }
        }
        //这块是对SQL进行解析，并将解析结果遍历封装在SQLStatement对象中
        SQLStatement result = new SQLParseKernel(ParseRuleRegistry.getInstance(), databaseType, sql).parse();
        if (useCache) {
            cache.put(sql, result);
        }
        return result;
    }
}
