package ru.yoomoney.tech.dbqueue.dao;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public interface Database {
    interface RowMapper<T> {
        T mapToObject(ResultSet rs) throws Exception;
    }

    <T> T selectOne(String selectQuery,
                    Map<String, Object> parameterMap,
                    Class<T> klass);

    default <T> T selectOne(String selectQuery,
                    Class<T> klass) {
        return selectOne(selectQuery, Collections.emptyMap(), klass);
    }

    <T> T selectOne(String selectQuery,
                    Map<String, Object> parameterMap,
                    RowMapper<T> rowMapper);

    default <T> T selectOne(String selectQuery,
                            RowMapper<T> rowMapper) {
        return selectOne(selectQuery, Collections.emptyMap(), rowMapper);
    }

    <T> List<T> selectMany(String selectQuery,
                           Map<String, Object> parameterMap,
                           RowMapper<T> rowMapper);

    default <T> List<T> selectMany(String selectQuery,
                                   RowMapper<T> rowMapper) {
        return selectMany(selectQuery, Collections.emptyMap(), rowMapper);
    }

    int insertOne(String insertQuery,
                  Map<String, Object> parameterMap);

    int update(String updateQuery,
               Map<String, Object> parameterMap);

    default int update(String updateQuery) {
        return update(updateQuery, Collections.emptyMap());
    }

    <T> T transact(Supplier<T> runsWithinTransaction);

    default void transact(Runnable runsWithinTransaction) {
        transact(() -> {
            runsWithinTransaction.run();
            return null;
        });
    }

    ;
}
