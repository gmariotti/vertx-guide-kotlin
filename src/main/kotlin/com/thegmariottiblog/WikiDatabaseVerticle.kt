package com.thegmariottiblog

import com.thegmariottiblog.ErrorCodes.BAD_ACTION
import com.thegmariottiblog.ErrorCodes.DB_ERROR
import com.thegmariottiblog.ErrorCodes.NO_ACTION_SPECIFIED
import com.thegmariottiblog.SqlQuery.ALL_PAGES
import com.thegmariottiblog.SqlQuery.CREATE_PAGE
import com.thegmariottiblog.SqlQuery.CREATE_PAGES_TABLE
import com.thegmariottiblog.SqlQuery.DELETE_PAGE
import com.thegmariottiblog.SqlQuery.GET_PAGE
import com.thegmariottiblog.SqlQuery.SAVE_PAGE
import io.vertx.core.AsyncResult
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.SQLConnection
import io.vertx.ext.sql.UpdateResult
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.experimental.launch
import org.slf4j.LoggerFactory
import java.io.FileInputStream
import java.util.Properties

class WikiDatabaseVerticle : CoroutineVerticle() {
    private val sqlQueries = mutableMapOf<SqlQuery, String>()

    private lateinit var client: JDBCClient

    override suspend fun start() {
        log.info("Initializing WikiDatabaseVerticle...")
        loadSqlQueries()
        prepareDatabase()
        log.info("WikiDatabaseVerticle initialized...")
    }

    private suspend fun prepareDatabase() {
        client = JDBCClient.createShared(vertx, json {
            obj(
                "url" to config.getString(CONFIG_WIKIDB_JDBC_URL, "jdbc:mariadb://localhost:3306/db?user=root"),
                "driver_class" to config.getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, "org.mariadb.jdbc.Driver")
            )
        })

        val connection = awaitResult<SQLConnection> { client.getConnection(it) }
        val result = connection.use { conn ->
            awaitEvent<AsyncResult<Void>> { conn.execute(SQL_CREATE_PAGE_TABLE, it) }
        }
        if (result.failed()) {
            log.error("Database preparation error", result.cause())
            throw result.cause()
        }

        vertx.eventBus()
            .consumer<JsonObject>(config.getString(CONFIG_WIKIDB_QUEUE, WIKIDB_QUEUE)) {
                launch(vertx.dispatcher()) { onMessage(it) }
            }
    }

    override suspend fun stop() {
        client.close()
    }

    private suspend fun onMessage(message: Message<JsonObject>) {
        if (!message.headers().contains("action")) {
            log.error(
                "No action header specified for message with headers {} and body {}",
                message.headers(),
                message.body().encodePrettily()
            )
            message.fail(NO_ACTION_SPECIFIED.ordinal, "No action header specified")
            return
        }

        val action = message.headers().get("action")
        log.info("Executing action: $action")
        when (action) {
            "all-pages" -> fetchAllPages(message)
            "get-page" -> fetchPage(message)
            "create-page" -> createPage(message)
            "save-page" -> savePage(message)
            "delete-page" -> deletePage(message)
            else -> message.fail(BAD_ACTION.ordinal, "Bad action: $action")
        }
    }

    private suspend fun fetchAllPages(message: Message<JsonObject>) {
        val event = awaitEvent<AsyncResult<ResultSet>> {
            client.query(sqlQueries[ALL_PAGES], it)
        }
        if (event.failed()) reportQueryError(message, event.cause())
        else {
            val pages = event.result().results.map { it.getString(0) }
            message.reply(JsonObject().put("pages", JsonArray(pages)))
        }
    }

    private suspend fun fetchPage(message: Message<JsonObject>) {
        val requestedPage = message.body().getString("page")
        val params = JsonArray().add(requestedPage)

        val event = awaitEvent<AsyncResult<ResultSet>> {
            client.queryWithParams(sqlQueries[GET_PAGE], params, it)
        }
        if (event.failed()) reportQueryError(message, event.cause())
        else {
            val response = JsonObject()
            val resultSet = event.result()
            if (resultSet.numRows == 0) {
                response.put("found", false)
            } else {
                response.put("found", true)
                val row = resultSet.results[0]
                response.put("id", row.getInteger(0))
                response.put("rawContent", row.getString(1))
            }
            message.reply(response)
        }
    }

    private suspend fun createPage(message: Message<JsonObject>) {
        val request = message.body()
        val data = JsonArray()
            .add(request.getString("title"))
            .add(request.getString("markdown"))

        val event = awaitEvent<AsyncResult<UpdateResult>> {
            client.updateWithParams(sqlQueries[CREATE_PAGE], data, it)
        }
        if (event.succeeded()) message.reply("ok")
        else reportQueryError(message, event.cause())
    }

    private suspend fun savePage(message: Message<JsonObject>) {
        val request = message.body()
        val data = JsonArray()
            .add(request.getString("markdown"))
            .add(request.getString("id"))

        val event = awaitEvent<AsyncResult<UpdateResult>> {
            client.updateWithParams(sqlQueries[SAVE_PAGE], data, it)
        }
        if (event.succeeded()) message.reply("ok")
        else reportQueryError(message, event.cause())
    }

    private suspend fun deletePage(message: Message<JsonObject>) {
        val data = JsonArray().add(message.body().getString("id"))

        val event = awaitEvent<AsyncResult<UpdateResult>> {
            client.updateWithParams(sqlQueries[DELETE_PAGE], data, it)
        }
        if (event.succeeded()) message.reply("ok")
        else reportQueryError(message, event.cause())
    }

    private fun reportQueryError(message: Message<JsonObject>, cause: Throwable) {
        log.error("Database query error", cause)
        message.fail(DB_ERROR.ordinal, cause.message)
    }

    private fun loadSqlQueries() {
        val queriesFile = config.getString(CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE)
        val queriesInputStream = queriesFile?.let { FileInputStream(it) }
            ?: javaClass.getResourceAsStream("/db-queries.properties")

        val queriesProps = Properties().apply {
            this.load(queriesInputStream)
            queriesInputStream.close()
        }

        sqlQueries.apply {
            put(CREATE_PAGES_TABLE, queriesProps.getProperty("create-page-table"))
            put(ALL_PAGES, queriesProps.getProperty("all-pages"))
            put(GET_PAGE, queriesProps.getProperty("get-page"))
            put(CREATE_PAGE, queriesProps.getProperty("create-page"))
            put(SAVE_PAGE, queriesProps.getProperty("save-page"))
            put(DELETE_PAGE, queriesProps.getProperty("delete-page"))
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(WikiDatabaseVerticle::class.java)
    }
}

enum class ErrorCodes {
    NO_ACTION_SPECIFIED,
    BAD_ACTION,
    DB_ERROR
}

private enum class SqlQuery {
    CREATE_PAGES_TABLE,
    ALL_PAGES,
    GET_PAGE,
    CREATE_PAGE,
    SAVE_PAGE,
    DELETE_PAGE
}
