package ru.yoomoney.tech.dbqueue.dao;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Database access object to manage tasks in the queue for PostgreSQL database type.
 *
 * @author Oleg Kandaurov
 * @since 09.07.2017
 */
public class PostgresQueueDao implements QueueDao {

    private final Map<QueueLocation, String> enqueueSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> deleteSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> reenqueueSqlCache = new ConcurrentHashMap<>();

    @Nonnull
    private final Database database;
    @Nonnull
    private final QueueTableSchema queueTableSchema;

    /**
     * Constructor
     *
     * @param database         Reference to JDBC database for the queue.
     * @param queueTableSchema Queue table scheme.
     */
    public PostgresQueueDao(@Nonnull Database database, @Nonnull QueueTableSchema queueTableSchema) {
        this.database = requireNonNull(database);
        this.queueTableSchema = requireNonNull(queueTableSchema);
    }

    @Override
    @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public long enqueue(@Nonnull QueueLocation location, @Nonnull EnqueueParams<String> enqueueParams) {
        requireNonNull(location);
        requireNonNull(enqueueParams);

        Map<String, Object> params = new HashMap<String, Object>() {
            {
                put("queueName", location.getQueueId().asString());
                put("payload", enqueueParams.getPayload());
                put("executionDelay", enqueueParams.getExecutionDelay().getSeconds());
            }
        };

        queueTableSchema.getExtFields().forEach(paramName -> params.put(paramName, null));
        params.putAll(enqueueParams.getExtData());

        return requireNonNull(database.insertOne(
                enqueueSqlCache.computeIfAbsent(location, this::createEnqueueSql), params));
    }


    @Override
    public boolean deleteTask(@Nonnull QueueLocation location, long taskId) {
        requireNonNull(location);

        final Map<String, Object> params = new HashMap<String, Object>() {{
            put("id", taskId);
            put("queueName", location.getQueueId().asString());
        }};

        int updatedRows = database.update(deleteSqlCache.computeIfAbsent(location, this::createDeleteSql), params);
        return updatedRows > 0L;
    }

    @Override
    public boolean reenqueue(@Nonnull QueueLocation location, long taskId, @Nonnull Duration executionDelay) {
        requireNonNull(location);
        requireNonNull(executionDelay);

        final Map<String, Object> params = new HashMap<String, Object>() {{
            put("id", taskId);
            put("queueName", location.getQueueId().asString());
            put("executionDelay", executionDelay.getSeconds());
        }};

        int updatedRows = database.update(reenqueueSqlCache.computeIfAbsent(location, this::createReenqueueSql), params);
        return updatedRows > 0L;
    }

    private String createEnqueueSql(@Nonnull QueueLocation location) {
        return "INSERT INTO " + location.getTableName() + "(" +
                (location.getIdSequence().map(ignored -> queueTableSchema.getIdField() + ",").orElse("")) +
                queueTableSchema.getQueueNameField() + "," +
                queueTableSchema.getPayloadField() + "," +
                queueTableSchema.getNextProcessAtField() + "," +
                queueTableSchema.getReenqueueAttemptField() + "," +
                queueTableSchema.getTotalAttemptField() +
                (queueTableSchema.getExtFields().isEmpty() ? "" :
                        queueTableSchema.getExtFields().stream().collect(Collectors.joining(", ", ", ", ""))) +
                ") VALUES " +
                "(" + location.getIdSequence().map(seq -> "nextval('" + seq + "'), ").orElse("") +
                ":queueName, :payload, now() + :executionDelay * INTERVAL '1 SECOND', 0, 0" +
                (queueTableSchema.getExtFields().isEmpty() ? "" : queueTableSchema.getExtFields().stream()
                        .map(field -> ":" + field).collect(Collectors.joining(", ", ", ", ""))) +
                ") RETURNING " + queueTableSchema.getIdField();
    }

    private String createDeleteSql(@Nonnull QueueLocation location) {
        return "DELETE FROM " + location.getTableName() + " WHERE " + queueTableSchema.getQueueNameField() +
                " = :queueName AND " + queueTableSchema.getIdField() + " = :id";
    }

    private String createReenqueueSql(@Nonnull QueueLocation location) {
        return "UPDATE " + location.getTableName() + " SET " + queueTableSchema.getNextProcessAtField() +
                " = now() + :executionDelay * INTERVAL '1 SECOND', " +
                queueTableSchema.getAttemptField() + " = 0, " +
                queueTableSchema.getReenqueueAttemptField() +
                " = " + queueTableSchema.getReenqueueAttemptField() + " + 1 " +
                "WHERE " + queueTableSchema.getIdField() + " = :id AND " +
                queueTableSchema.getQueueNameField() + " = :queueName";
    }

}
