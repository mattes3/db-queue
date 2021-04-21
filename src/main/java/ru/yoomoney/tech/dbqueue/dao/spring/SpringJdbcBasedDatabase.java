package ru.yoomoney.tech.dbqueue.dao.spring;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import ru.yoomoney.tech.dbqueue.dao.Database;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class SpringJdbcBasedDatabase implements Database {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    public SpringJdbcBasedDatabase(DataSource dataSource) {
        this.jdbcTemplate = new NamedParameterJdbcTemplate(requireNonNull(dataSource));
        this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        this.transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
    }

    @Override
    public <T> T selectOne(String selectQuery, Map<String, Object> parameterMap, Class<T> klass) {
        return jdbcTemplate.queryForObject(selectQuery, parameterMap, klass);
    }

    @Override
    public <T> T selectOne(String selectQuery,
                           Map<String, Object> parameterMap,
                           RowMapper<T> rowMapper) {

        return jdbcTemplate.queryForObject(selectQuery, parameterMap, (rs, rowNum) -> {
            try {
                return rowMapper.mapToObject(rs);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public <T> List<T> selectMany(String selectQuery,
                                  Map<String, Object> parameterMap,
                                  RowMapper<T> rowMapper) {
        return jdbcTemplate.query(selectQuery, parameterMap, (rs, rowNum) -> {
            try {
                return rowMapper.mapToObject(rs);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public int insertOne(String insertQuery,
                         Map<String, Object> parameterMap) {
        return jdbcTemplate.update(insertQuery, parameterMap);
    }

    @Override
    public int update(String updateQuery, Map<String, Object> parameterMap) {
        return jdbcTemplate.update(updateQuery, parameterMap);
    }

    @Override
    public <T> T transact(Supplier<T> runsWithinTransaction) {
        return transactionTemplate.execute((status) -> runsWithinTransaction.get());
    }
}
