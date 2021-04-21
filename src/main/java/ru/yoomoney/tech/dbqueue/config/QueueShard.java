package ru.yoomoney.tech.dbqueue.config;

import ru.yoomoney.tech.dbqueue.dao.Database;
import ru.yoomoney.tech.dbqueue.dao.QueueDao;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * Properties for connection to a database shard.
 *
 * @author Oleg Kandaurov
 * @since 13.08.2018
 */
public class QueueShard {

    @Nonnull
    private final DatabaseDialect databaseDialect;
    @Nonnull
    private final QueueShardId shardId;
    @Nonnull
    private final Database database;
    @Nonnull
    private final QueueTableSchema queueTableSchema;
    @Nonnull
    private final QueueDao queueDao;

    /**
     * Constructor
     *
     * @param databaseDialect  Database type (dialect)
     * @param queueTableSchema Queue table scheme.
     * @param shardId          Shard identifier.
     * @param database         Reference to JDBC data source
     */
    public QueueShard(@Nonnull DatabaseDialect databaseDialect,
                      @Nonnull QueueTableSchema queueTableSchema,
                      @Nonnull QueueShardId shardId,
                      @Nonnull Database database) {
        this.databaseDialect = requireNonNull(databaseDialect);
        this.shardId = requireNonNull(shardId);
        this.database = requireNonNull(database);
        this.queueTableSchema = requireNonNull(queueTableSchema);
        this.queueDao = QueueDao.Factory.create(databaseDialect, database, queueTableSchema);
    }

    /**
     * Get shard identifier.
     *
     * @return Shard identifier.
     */
    @Nonnull
    public QueueShardId getShardId() {
        return shardId;
    }

    /**
     * Get reference to the data source for that shard.
     *
     * @return Reference to JDBC data source.
     */
    @Nonnull
    public Database getDatabase() {
        return database;
    }

    /**
     * Get reference to database access object to work with queue storage on that shard.
     *
     * @return Reference to database access object to work with the queue.
     */
    @Nonnull
    public QueueDao getQueueDao() {
        return queueDao;
    }

    /**
     * Get database type for that shard.
     *
     * @return Database type.
     */
    @Nonnull
    public DatabaseDialect getDatabaseDialect() {
        return databaseDialect;
    }

    /**
     * Get queue table schema for that shard.
     *
     * @return Queue table schema.
     */
    @Nonnull
    public QueueTableSchema getQueueTableSchema() {
        return queueTableSchema;
    }


    public <T> T transact(Supplier<T> runWithinTransaction) {
        return database.transact(runWithinTransaction);
    }
}
