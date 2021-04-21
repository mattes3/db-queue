package ru.yoomoney.tech.dbqueue.dao;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Database access object to manage tasks in the queue for Oracle database type.
 *
 * @author Oleg Kandaurov
 * @since 15.05.2020
 */
public class Oracle11QueueDao implements QueueDao {

    private final Map<QueueLocation, String> enqueueSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> deleteSqlCache = new ConcurrentHashMap<>();
    private final Map<QueueLocation, String> reenqueueSqlCache = new ConcurrentHashMap<>();
    private final Map<String, String> nextSequenceSqlCache = new ConcurrentHashMap<>();

    @Nonnull
    private final Database database;
    @Nonnull
    private final QueueTableSchema queueTableSchema;

    /**
     * Constructor
     *
     * @param database       Reference to JDBC data source for the queue.
     * @param queueTableSchema Queue table scheme.
     */
    public Oracle11QueueDao(Database database, QueueTableSchema queueTableSchema) {
        this.database = database;
        this.queueTableSchema = requireNonNull(queueTableSchema);
    }

    @Override
    @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE", "SQL_INJECTION_SPRING_JDBC"})
    public long enqueue(@Nonnull QueueLocation location, @Nonnull EnqueueParams<String> enqueueParams) {
        requireNonNull(location);
        requireNonNull(enqueueParams);

        String idSequence = location.getIdSequence()
                .orElseThrow(() -> new IllegalStateException("id sequence must be specified for oracle 11g database"));

        Long generatedId = Objects.requireNonNull(database.selectOne(
                nextSequenceSqlCache.computeIfAbsent(idSequence, this::createNextSequenceSql), Long.class));

        Map<String, Object> params = new HashMap<String, Object>() {
            {
                put("queueName", location.getQueueId().asString());
                put("payload", enqueueParams.getPayload());
                put("executionDelay", enqueueParams.getExecutionDelay().getSeconds());
                put("id", generatedId);
            }
        };

        queueTableSchema.getExtFields().forEach(paramName -> params.put(paramName, null));
        params.putAll(enqueueParams.getExtData());

        database.update(enqueueSqlCache.computeIfAbsent(location, this::createEnqueueSql), params);
        return generatedId;
    }


    @Override
    public boolean deleteTask(@Nonnull QueueLocation location, long taskId) {
        requireNonNull(location);

        final Map<String, Object> mapSqlParameterSource = new HashMap<String, Object>() {
            {
                put("id", taskId);
                put("queueName", location.getQueueId().asString());
            }
        };
        int updatedRows = database.update(deleteSqlCache.computeIfAbsent(location, this::createDeleteSql),
                mapSqlParameterSource);
        return updatedRows != 0;
    }

    @Override
    public boolean reenqueue(@Nonnull QueueLocation location, long taskId, @Nonnull Duration executionDelay) {
        requireNonNull(location);
        requireNonNull(executionDelay);
        final Map<String, Object> mapSqlParameterSource = new HashMap<String, Object>() {
            {
                put("id", taskId);
                put("queueName", location.getQueueId().asString());
                put("executionDelay", executionDelay.getSeconds());
            }
        };
        int updatedRows = database.update(reenqueueSqlCache.computeIfAbsent(location, this::createReenqueueSql),
                mapSqlParameterSource);
        return updatedRows != 0;
    }

    private String createDeleteSql(@Nonnull QueueLocation location) {
        return "DELETE FROM " + location.getTableName() + " WHERE " + queueTableSchema.getQueueNameField() +
                " = :queueName AND " + queueTableSchema.getIdField() + " = :id";
    }

    private String createEnqueueSql(@Nonnull QueueLocation location) {
        return "INSERT INTO " + location.getTableName() + "(" +
                queueTableSchema.getIdField() + "," +
                queueTableSchema.getQueueNameField() + "," +
                queueTableSchema.getPayloadField() + "," +
                queueTableSchema.getNextProcessAtField() + "," +
                queueTableSchema.getReenqueueAttemptField() + "," +
                queueTableSchema.getTotalAttemptField() +
                (queueTableSchema.getExtFields().isEmpty() ? "" :
                        queueTableSchema.getExtFields().stream().collect(Collectors.joining(", ", ", ", ""))) +
                ") VALUES " +
                "(:id, :queueName, :payload, CURRENT_TIMESTAMP + :executionDelay * INTERVAL '1' SECOND, 0, 0" +
                (queueTableSchema.getExtFields().isEmpty() ? "" : queueTableSchema.getExtFields().stream()
                        .map(field -> ":" + field).collect(Collectors.joining(", ", ", ", ""))) +
                ")";
    }

    private String createReenqueueSql(@Nonnull QueueLocation location) {
        return "UPDATE " + location.getTableName() + " SET " + queueTableSchema.getNextProcessAtField() +
                " = CURRENT_TIMESTAMP + :executionDelay * INTERVAL '1' SECOND, " +
                queueTableSchema.getAttemptField() + " = 0, " +
                queueTableSchema.getReenqueueAttemptField() +
                " = " + queueTableSchema.getReenqueueAttemptField() + " + 1 " +
                "WHERE " + queueTableSchema.getIdField() + " = :id AND " +
                queueTableSchema.getQueueNameField() + " = :queueName";
    }

    private String createNextSequenceSql(String idSequence) {
        return "SELECT " + idSequence + ".nextval FROM dual";
    }

}
