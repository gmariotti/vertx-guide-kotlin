package com.thegmariottiblog

import EMPTY_PAGE_MARKDOWN
import ch.vorburger.mariadb4j.DB
import com.github.rjeschke.txtmark.Processor
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpServer
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.SQLConnection
import io.vertx.ext.sql.UpdateResult
import io.vertx.ext.web.Route
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitResult
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.experimental.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

class MainVerticle : CoroutineVerticle() {

    private val db = DB.newEmbeddedDB(3306).also { it.start() }
    private val templateEngine = FreeMarkerTemplateEngine.create()

    private lateinit var client: JDBCClient

    override suspend fun start() {
        initializeDatabase()
        prepareDatabase()
        startHttpServer()
    }

    private suspend fun initializeDatabase() {
        JDBCClient.createShared(vertx, json {
            obj(
                "url" to "jdbc:mariadb://localhost:3306?user=root",
                "driver_class" to "org.mariadb.jdbc.Driver"
            )
        }).apply {
            awaitResult<SQLConnection> { this.getConnection(it) }
                .use { conn ->
                    awaitResult<Void> { conn.execute("CREATE DATABASE IF NOT EXISTS db", it) }
                }
        }.close()
    }

    private suspend fun prepareDatabase() {
        client = JDBCClient.createShared(vertx, json {
            obj(
                "url" to "jdbc:mariadb://localhost:3306/db?user=root",
                "driver_class" to "org.mariadb.jdbc.Driver"
            )
        })

        val connection = awaitResult<SQLConnection> { client.getConnection(it) }
        connection.use {
            awaitResult<Void> {
                connection.execute(SQL_CREATE_PAGE_TABLE, it)
            }
        }
    }

    private suspend fun startHttpServer() {
        val router = Router.router(vertx)
        router.get("/") coroutineHandler { indexHandler(it) }
        router.get("/wiki/:page") coroutineHandler { pageRenderingHandler(it) }
        router.post().handler(BodyHandler.create())
        router.post("/save") coroutineHandler { pageUpdateHandler(it) }
        router.post("/create") coroutineHandler { pageCreateHandler(it) }
        router.post("/delete") coroutineHandler { pageDeletionHandler(it) }
        router.get("/ping") coroutineHandler { getPing(it) }

        awaitResult<HttpServer> {
            vertx.createHttpServer()
                .requestHandler(router::accept)
                .listen(config.getInteger("http.port", 8080), it)
        }
    }

    private suspend fun indexHandler(context: RoutingContext) {
        val result = awaitResult<SQLConnection> { client.getConnection(it) }
            .use { conn -> awaitResult<ResultSet> { conn.query(SQL_ALL_PAGES, it) } }
        val pages = result.results
            .map { it.getString(0) }
            .sorted()

        with(context) {
            put("title", "Wiki home")
            put("pages", pages)
            val buffer = awaitResult<Buffer> {
                templateEngine.render(
                    this,
                    "templates",
                    "/index.ftl",
                    it
                )
            }
            response().putHeader("Content-Type", "text/html")
            response().end(buffer)
        }
    }

    private suspend fun pageRenderingHandler(context: RoutingContext) {
        val page = context.request().getParam("page")

        log.info("rendering page=$page")

        val result = awaitResult<SQLConnection> { client.getConnection(it) }
            .use { conn ->
                awaitResult<ResultSet> {
                    conn.queryWithParams(SQL_GET_PAGE, JsonArray().add(page), it)
                }
            }

        val row = result.results
            .firstOrNull()
            ?: JsonArray().add(-1).add(EMPTY_PAGE_MARKDOWN)
        val id = row.getInteger(0)
        val rawContent = row.getString(1)

        with(context) {
            put("title", page)
            put("id", id)
            put("newPage", if (result.results.size == 0) "yes" else "no")
            put("rawContent", rawContent)
            put("content", Processor.process(rawContent))
            put("timestamp", LocalDateTime.now().toString())

            val buffer = awaitResult<Buffer> {
                templateEngine.render(
                    this,
                    "templates",
                    "/page.ftl",
                    it
                )
            }
            response().putHeader("Content-Type", "text/html")
            response().end(buffer)
        }
    }

    private suspend fun pageUpdateHandler(context: RoutingContext) = with(context) {
        val id = request().getParam("id")
        val title = request().getParam("title")
        val markdown = request().getParam("markdown")
        val newPage = "yes" == request().getParam("newPage")

        log.info("update page id=$id and title=$title")

        awaitResult<SQLConnection> { client.getConnection(it) }
            .use { conn ->
                val sql = if (newPage) SQL_CREATE_PAGE else SQL_SAVE_PAGE
                val params = JsonArray()
                if (newPage) params.add(title).add(markdown)
                else params.add(markdown).add(id)
                awaitResult<UpdateResult> { conn.updateWithParams(sql, params, it) }
            }

        response().setStatusCode(303)
            .putHeader("Location", "/wiki/$title")
            .end()
    }

    private fun pageCreateHandler(context: RoutingContext) = with(context) {
        val pageName = request().getParam("name")

        log.info("create page with name=$pageName")

        val location = if (pageName.isNullOrEmpty()) "/" else "/wiki/$pageName"
        response().setStatusCode(303)
            .putHeader("Location", location)
            .end()
    }

    private suspend fun pageDeletionHandler(context: RoutingContext) {
        val id = context.request().getParam("id")

        log.info("delete page with id=$id")

        awaitResult<SQLConnection> { client.getConnection(it) }
            .use { conn ->
                awaitResult<UpdateResult> { conn.updateWithParams(SQL_DELETE_PAGE, JsonArray().add(id), it) }
            }

        context.response()
            .setStatusCode(303)
            .putHeader("Location", "/")
            .end()
    }

    private fun getPing(context: RoutingContext) = context
        .response()
        .setStatusCode(200)
        .end(JsonObject.mapFrom(
            object {
                val ping = "pong"
            }
        ).toString())

    override suspend fun stop() {
        client.close()
        db.stop()
    }

    companion object {
        private val log = LoggerFactory.getLogger(MainVerticle::class.java)
    }
}

/**
 * An extension method for simplifying coroutines usage with Vert.x Web routers
 */
infix fun Route.coroutineHandler(fn: suspend (RoutingContext) -> Unit) {
    handler { ctx ->
        launch(ctx.vertx().dispatcher()) {
            try {
                fn(ctx)
            } catch (e: Exception) {
                ctx.fail(e)
            }
        }
    }
}