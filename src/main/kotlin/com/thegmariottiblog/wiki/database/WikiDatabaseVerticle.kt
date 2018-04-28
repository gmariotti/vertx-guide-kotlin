package com.thegmariottiblog.wiki.database

import com.thegmariottiblog.wiki.CONFIG_WIKIDB_JDBC_DRIVER_CLASS
import com.thegmariottiblog.wiki.CONFIG_WIKIDB_JDBC_URL
import com.thegmariottiblog.wiki.CONFIG_WIKIDB_QUEUE
import com.thegmariottiblog.wiki.CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE
import com.thegmariottiblog.wiki.database.SqlQuery.ALL_PAGES
import com.thegmariottiblog.wiki.database.SqlQuery.ALL_PAGES_DATA
import com.thegmariottiblog.wiki.database.SqlQuery.CREATE_PAGE
import com.thegmariottiblog.wiki.database.SqlQuery.CREATE_PAGES_TABLE
import com.thegmariottiblog.wiki.database.SqlQuery.DELETE_PAGE
import com.thegmariottiblog.wiki.database.SqlQuery.GET_PAGE
import com.thegmariottiblog.wiki.database.SqlQuery.SAVE_PAGE
import io.vertx.core.AsyncResult
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.serviceproxy.ServiceBinder
import org.slf4j.LoggerFactory
import java.io.FileInputStream
import java.util.Properties

class WikiDatabaseVerticle : CoroutineVerticle() {
    // otherwise throws a not initialized exception for the context
    private val sqlQueries by lazy { loadSqlQueries() }

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

        val result = awaitEvent<AsyncResult<WikiDatabaseService>> {
            create(
                dbClient = client,
                sqlQueries = sqlQueries,
                dispatcher = vertx.dispatcher(),
                readyHandler = it
            )
        }
        if (result.failed()) {
            log.error("Database preparation error", result.cause())
            throw result.cause()
        }
        ServiceBinder(vertx)
            .setAddress(CONFIG_WIKIDB_QUEUE)
            .register(WikiDatabaseService::class.java, result.result())
    }

    override suspend fun stop() {
        client.close()
    }

    private fun loadSqlQueries(): Map<SqlQuery, String> {
        val queriesFile = config.getString(CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE)
        val queriesInputStream = queriesFile?.let { FileInputStream(it) }
            ?: javaClass.getResourceAsStream("/db-queries.properties")

        val queriesProps = Properties().apply {
            this.load(queriesInputStream)
            queriesInputStream.close()
        }

        return mapOf(
            CREATE_PAGES_TABLE to queriesProps.getProperty("create-page-table"),
            ALL_PAGES to queriesProps.getProperty("all-pages"),
            GET_PAGE to queriesProps.getProperty("get-page"),
            CREATE_PAGE to queriesProps.getProperty("create-page"),
            SAVE_PAGE to queriesProps.getProperty("save-page"),
            DELETE_PAGE to queriesProps.getProperty("delete-page"),
            ALL_PAGES_DATA to queriesProps.getProperty("all-pages-data")
        )
    }

    companion object {
        private val log = LoggerFactory.getLogger(WikiDatabaseVerticle::class.java)
    }
}
