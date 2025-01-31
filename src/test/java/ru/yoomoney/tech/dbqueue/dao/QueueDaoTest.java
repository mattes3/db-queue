package ru.yoomoney.tech.dbqueue.dao;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import ru.yoomoney.tech.dbqueue.api.EnqueueParams;
import ru.yoomoney.tech.dbqueue.config.QueueTableSchema;
import ru.yoomoney.tech.dbqueue.settings.QueueId;
import ru.yoomoney.tech.dbqueue.settings.QueueLocation;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.*;

/**
 * @author Oleg Kandaurov
 * @author Behrooz Shabani
 * @since 25.01.2020
 */
@Ignore
public abstract class QueueDaoTest {

    protected final String tableName;
    protected final QueueTableSchema tableSchema;
    protected final Database database;

    public QueueDao queueDao;

    public QueueDaoTest(QueueDao queueDao, String tableName, QueueTableSchema tableSchema, Database database) {
        this.queueDao = queueDao;
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.database = database;
    }

    @Test
    public void enqueue_should_accept_null_values() throws Exception {
        QueueLocation location = generateUniqueLocation();
        long enqueueId = queueDao.enqueue(location, new EnqueueParams<>());
        Assert.assertThat(enqueueId, not(equalTo(0)));
    }

    @Test
    public void enqueue_should_save_all_values() throws Exception {
        QueueLocation location = generateUniqueLocation();
        String payload = "{}";
        Duration executionDelay = Duration.ofHours(1L);
        ZonedDateTime beforeExecution = ZonedDateTime.now();
        long enqueueId = database.transact(() -> queueDao.enqueue(location, EnqueueParams.create(payload)
                .withExecutionDelay(executionDelay)));
        database.selectOne("select * from " + tableName + " where " + tableSchema.getIdField() + "=" + enqueueId, rs -> {
            ZonedDateTime afterExecution = ZonedDateTime.now();
            Assert.assertThat(rs.getString(tableSchema.getPayloadField()), equalTo(payload));
            ZonedDateTime nextProcessAt = ZonedDateTime.ofInstant(rs.getTimestamp(tableSchema.getNextProcessAtField()).toInstant(),
                    ZoneId.systemDefault());
            Assert.assertThat(nextProcessAt.isAfter(beforeExecution.plus(executionDelay)), equalTo(true));
            Assert.assertThat(nextProcessAt.isBefore(afterExecution.plus(executionDelay)), equalTo(true));
            ZonedDateTime createdAt = ZonedDateTime.ofInstant(rs.getTimestamp(tableSchema.getCreatedAtField()).toInstant(),
                    ZoneId.systemDefault());
            Assert.assertThat(createdAt.isAfter(beforeExecution), equalTo(true));
            Assert.assertThat(createdAt.isBefore(afterExecution), equalTo(true));

            long reenqueueAttempt = rs.getLong(tableSchema.getReenqueueAttemptField());
            Assert.assertFalse(rs.wasNull());
            Assert.assertEquals(0L, reenqueueAttempt);

            return new Object();
        });
    }

    @Test
    public void delete_should_return_false_when_no_deletion() throws Exception {
        QueueLocation location = generateUniqueLocation();
        Boolean deleteResult = database.transact(() -> queueDao.deleteTask(location, 0L));
        Assert.assertThat(deleteResult, equalTo(false));
    }

    @Test
    public void delete_should_return_true_when_deletion_occur() throws Exception {
        QueueLocation location = generateUniqueLocation();
        Long enqueueId = database.transact(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        Boolean deleteResult = database.transact(() -> queueDao.deleteTask(location, enqueueId));
        Assert.assertThat(deleteResult, equalTo(true));
        Object obj = database.selectOne("select * from " + tableName + " where " + tableSchema.getIdField() + "=" + enqueueId, rs -> {
            return new Object();
        });
        Assert.assertThat(obj, nullValue());
    }

    @Test
    public void reenqueue_should_update_next_process_time() throws Exception {
        QueueLocation location = generateUniqueLocation();
        Long enqueueId = database.transact(() ->
                queueDao.enqueue(location, new EnqueueParams<>()));

        ZonedDateTime beforeExecution = ZonedDateTime.now();
        Duration executionDelay = Duration.ofHours(1L);
        Boolean reenqueueResult = database.transact(() -> queueDao.reenqueue(location, enqueueId, executionDelay));
        Assert.assertThat(reenqueueResult, equalTo(true));
        List<Object> objects = database.selectMany("select * from " + tableName + " where " + tableSchema.getIdField() + "=" + enqueueId, rs -> {
            ZonedDateTime afterExecution = ZonedDateTime.now();
            ZonedDateTime nextProcessAt = ZonedDateTime.ofInstant(rs.getTimestamp(tableSchema.getNextProcessAtField()).toInstant(),
                    ZoneId.systemDefault());

            Assert.assertThat(nextProcessAt.isAfter(beforeExecution.plus(executionDelay)), equalTo(true));
            Assert.assertThat(nextProcessAt.isBefore(afterExecution.plus(executionDelay)), equalTo(true));
            return new Object();
        });
        Assert.assertThat(objects.size() > 1, is(equalTo(true)));
    }

    @Test
    public void reenqueue_should_reset_attempts() throws Exception {
        QueueLocation location = generateUniqueLocation();
        Long enqueueId = database.transact(() ->
                queueDao.enqueue(location, new EnqueueParams<>()));
        database.transact(() -> {
            database.update("update " + tableName + " set " + tableSchema.getAttemptField() + "=10 where " + tableSchema.getIdField() + "=" + enqueueId);
        });

        Object object = database.selectOne("select * from " + tableName + " where " + tableSchema.getIdField() + "=" + enqueueId, rs -> {
            Assert.assertThat(rs.getLong(tableSchema.getAttemptField()), equalTo(10L));
            return new Object();
        });
        Assert.assertThat(object, notNullValue());

        Boolean reenqueueResult = database.transact(() ->
                queueDao.reenqueue(location, enqueueId, Duration.ofHours(1L)));

        Assert.assertThat(reenqueueResult, equalTo(true));

        object = database.selectOne("select * from " + tableName + " where " + tableSchema.getIdField() + "=" + enqueueId, rs -> {
            Assert.assertThat(rs.getLong(tableSchema.getAttemptField()), equalTo(0L));
            return new Object();
        });
        Assert.assertThat(object, notNullValue());
    }

    @Test
    public void reenqueue_should_increment_reenqueue_attempts() {
        QueueLocation location = generateUniqueLocation();

        Long enqueueId = database.transact(() -> queueDao.enqueue(location, new EnqueueParams<>()));

        Object object = database.selectOne("select * from " + tableName + " where " + tableSchema.getIdField() + "=" + enqueueId, rs -> {
            Assert.assertThat(rs.getLong(tableSchema.getReenqueueAttemptField()), equalTo(0L));
            return new Object();
        });
        Assert.assertThat(object, notNullValue());

        Boolean reenqueueResult = database.transact(() -> queueDao.reenqueue(location, enqueueId, Duration.ofHours(1L)));

        Assert.assertThat(reenqueueResult, equalTo(true));
        object = database.selectOne("select * from " + tableName + " where " + tableSchema.getIdField() + "=" + enqueueId, rs -> {
            Assert.assertThat(rs.getLong(tableSchema.getReenqueueAttemptField()), equalTo(1L));
            return new Object();
        });
        Assert.assertThat(object, notNullValue());
    }

    @Test
    public void reenqueue_should_return_false_when_no_update() throws Exception {
        QueueLocation location = generateUniqueLocation();
        Boolean reenqueueResult = database.transact(() ->
                queueDao.reenqueue(location, 0L, Duration.ofHours(1L)));
        Assert.assertThat(reenqueueResult, equalTo(false));
    }

    protected QueueLocation generateUniqueLocation() {
        return QueueLocation.builder().withTableName(tableName)
                .withQueueId(new QueueId("test-queue-" + UUID.randomUUID())).build();
    }

}
