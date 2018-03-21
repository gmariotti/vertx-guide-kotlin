package com.thegmariottiblog.wiki.database

import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.ext.jdbc.JDBCClient
import kotlinx.coroutines.experimental.CoroutineDispatcher

fun create(
    dbClient: JDBCClient,
    sqlQueries: Map<SqlQuery, String>,
    dispatcher: CoroutineDispatcher,
    readyHandler: Handler<AsyncResult<WikiDatabaseService>>
) = WikiDatabaseServiceImpl(
    dbClient = dbClient,
    sqlQueries = sqlQueries,
    dispatcher = dispatcher,
    readyHandler = readyHandler
)

fun createProxy(vertx: Vertx, address: String) = WikiDatabaseServiceVertxEBProxy(vertx, address)