package ru.yoomoney.tech.dbqueue.internal.pick;

import org.springframework.jdbc.core.JdbcOperations;
import ru.yoomoney.tech.dbqueue.api.TaskRecord;
import ru.yoomoney.tech.dbqueue.config.DatabaseDialect;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * DB interaction class for task selection
 *
 * @author Oleg Kandaurov
 * @since 06.10.2019
 */
public interface QueuePickTaskDao {

    /**
     * Select the next task from the queue
     *
     * @param location queue location
     * @return task to process or null if none was found
     */
    @Nullable
    TaskRecord pickTask(@Nonnull QueueLocation location);

    /**
     * Factory for creating database-specific DAOs for fetching queues
     */
    class Factory {

        /**
         * Create a dao instance to select tasks from the queue depending on the type of database
         *
         * @param databaseDialect  database dialect
         * @param jdbcTemplate     spring jdbc template
         * @param queueTableSchema queue table schema
         * @param pickTaskSettings task selection settings
         * @return dao to work with queues
         */
        public static QueuePickTaskDao create(@Nonnull DatabaseDialect databaseDialect,
                                              @Nonnull QueueTableSchema queueTableSchema,
                                              @Nonnull JdbcOperations jdbcTemplate,
                                              @Nonnull PickTaskSettings pickTaskSettings) {
            requireNonNull(databaseDialect);
            requireNonNull(queueTableSchema);
            requireNonNull(jdbcTemplate);
            requireNonNull(pickTaskSettings);
            switch (databaseDialect) {
                case POSTGRESQL:
                    return new PostgresQueuePickTaskDao(jdbcTemplate, queueTableSchema, pickTaskSettings);
                case MSSQL:
                    return new MssqlQueuePickTaskDao(jdbcTemplate, queueTableSchema, pickTaskSettings);
                case ORACLE_11G:
                    return new Oracle11QueuePickTaskDao(jdbcTemplate, queueTableSchema, pickTaskSettings);
                default:
                    throw new IllegalArgumentException("unsupported database kind: " + databaseDialect);
            }
        }
    }
}
