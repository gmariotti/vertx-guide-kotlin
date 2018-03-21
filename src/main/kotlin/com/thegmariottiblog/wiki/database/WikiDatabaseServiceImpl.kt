package com.thegmariottiblog.wiki.database

import com.thegmariottiblog.wiki.database.SqlQuery.ALL_PAGES
import com.thegmariottiblog.wiki.database.SqlQuery.CREATE_PAGE
import com.thegmariottiblog.wiki.database.SqlQuery.CREATE_PAGES_TABLE
import com.thegmariottiblog.wiki.database.SqlQuery.DELETE_PAGE
import com.thegmariottiblog.wiki.database.SqlQuery.GET_PAGE
import com.thegmariottiblog.wiki.database.SqlQuery.SAVE_PAGE
import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.SQLConnection
import io.vertx.ext.sql.UpdateResult
import io.vertx.kotlin.coroutines.awaitEvent
import kotlinx.coroutines.experimental.CoroutineDispatcher
import kotlinx.coroutines.experimental.launch
import org.slf4j.LoggerFactory

class WikiDatabaseServiceImpl(
    private val dbClient: JDBCClient,
    private val sqlQueries: Map<SqlQuery, String>,
    private val dispatcher: CoroutineDispatcher,
    readyHandler: Handler<AsyncResult<WikiDatabaseService>>
) : WikiDatabaseService {
    init {
        launch(dispatcher) {
            val event = awaitEvent<AsyncResult<SQLConnection>> { dbClient.getConnection(it) }
            if (event.failed()) {
                log.error("Could not open a database connection", event.cause())
                readyHandler.handle(Future.failedFuture(event.cause()))
            } else {
                val connection = event.result()
                val create = connection.use {
                    awaitEvent<AsyncResult<Void>> { connection.execute(sqlQueries[CREATE_PAGES_TABLE], it) }
                }
                if (create.failed()) {
                    log.error("Database preparation error", create.cause())
                    readyHandler.handle(Future.failedFuture(create.cause()))
                } else {
                    readyHandler.handle(Future.succeededFuture(this@WikiDatabaseServiceImpl))
                }
            }
        }
    }

    override fun fetchAllPages(resultHandler: Handler<AsyncResult<JsonArray>>): WikiDatabaseService {
        launch(dispatcher) {
            val event = awaitEvent<AsyncResult<ResultSet>> { dbClient.query(sqlQueries[ALL_PAGES], it) }
            if (event.failed()) {
                log.error("Database query error", event.cause())
                throw event.cause()
            }
            val pages = JsonArray(
                event.result()
                    .results
                    .map { it.getString(0) }
                    .sorted()
                    .toList()
            )
            resultHandler.handle(Future.succeededFuture(pages))
        }
        return this
    }

    override fun fetchPage(name: String, resultHandler: Handler<AsyncResult<JsonObject>>): WikiDatabaseService {
        launch(dispatcher) {
            val fetch = awaitEvent<AsyncResult<ResultSet>> {
                dbClient.queryWithParams(
                    sqlQueries[GET_PAGE],
                    JsonArray().add(name),
                    it
                )
            }
            if (fetch.failed()) {
                log.error("Database query error", fetch.cause())
                throw fetch.cause()
            }
            val response = JsonObject()
            val result = fetch.result()
            if (result.numRows == 0) response.put("found", false)
            else {
                response.put("found", true)
                val row = result.results[0]
                response.put("id", row.getInteger(0))
                response.put("rawContent", row.getString(1))
            }
            resultHandler.handle(Future.succeededFuture(response))
        }
        return this
    }

    override fun createPage(
        title: String,
        markdown: String,
        resultHandler: Handler<AsyncResult<Void>>
    ): WikiDatabaseService {
        launch(dispatcher) {
            val data = JsonArray().add(title).add(markdown)
            val update = awaitEvent<AsyncResult<UpdateResult>> {
                dbClient.updateWithParams(sqlQueries[CREATE_PAGE], data, it)
            }
            if (update.failed()) {
                log.error("Database query error", update.cause())
                throw update.cause()
            }
            resultHandler.handle(Future.succeededFuture())
        }
        return this
    }

    override fun savePage(id: Int, markdown: String, resultHandler: Handler<AsyncResult<Void>>): WikiDatabaseService {
        launch(dispatcher) {
            val data = JsonArray().add(markdown).add(id)
            val save = awaitEvent<AsyncResult<UpdateResult>> {
                dbClient.updateWithParams(sqlQueries[SAVE_PAGE], data, it)
            }
            if (save.failed()) {
                log.error("Database query error", save.cause())
                throw save.cause()
            }
            resultHandler.handle(Future.succeededFuture())
        }
        return this
    }

    override fun deletePage(id: Int, resultHandler: Handler<AsyncResult<Void>>): WikiDatabaseService {
        launch(dispatcher) {
            val data = JsonArray().add(id)
            val delete = awaitEvent<AsyncResult<UpdateResult>> {
                dbClient.updateWithParams(sqlQueries[DELETE_PAGE], data, it)
            }
            if (delete.failed()) {
                log.error("Database query error", delete.cause())
                throw delete.cause()
            }
            resultHandler.handle(Future.succeededFuture())
        }
        return this
    }

    companion object {
        private val log = LoggerFactory.getLogger(WikiDatabaseServiceImpl::class.java)
    }
}