package com.thegmariottiblog.wiki.http

import EMPTY_PAGE_MARKDOWN
import com.github.rjeschke.txtmark.Processor
import com.thegmariottiblog.wiki.CONFIG_HTTP_SERVER_PORT
import com.thegmariottiblog.wiki.CONFIG_WIKIDB_QUEUE
import com.thegmariottiblog.wiki.WIKIDB_QUEUE
import com.thegmariottiblog.wiki.coroutineHandler
import com.thegmariottiblog.wiki.database.WikiDatabaseService
import com.thegmariottiblog.wiki.database.createProxy
import io.vertx.core.AsyncResult
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.http.HttpServer
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.awaitResult
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

class HttpServerVerticle : CoroutineVerticle() {
    private val templateEngine = FreeMarkerTemplateEngine.create()
    private lateinit var dbService: WikiDatabaseService
    private lateinit var wikiDbQueue: String

    override suspend fun start() {
        wikiDbQueue = config.getString(CONFIG_WIKIDB_QUEUE, WIKIDB_QUEUE)
        dbService = createProxy(vertx, wikiDbQueue)

        val server = vertx.createHttpServer()

        val router = Router.router(vertx)
        router.get("/") coroutineHandler { indexHandler(it) }
        router.get("/wiki/:page") coroutineHandler { pageRenderingHandler(it) }
        router.post().handler(BodyHandler.create())
        router.post("/save") coroutineHandler { pageUpdateHandler(it) }
        router.post("/create") coroutineHandler { pageCreateHandler(it) }
        router.post("/delete") coroutineHandler { pageDeletionHandler(it) }
        router.get("/ping") coroutineHandler { getPing(it) }

        val portNumber = config.getInteger(CONFIG_HTTP_SERVER_PORT, 8080)

        awaitEvent<HttpServer> {
            server.requestHandler(router::accept)
                .listen(portNumber) {
                    if (it.succeeded()) log.info("HTTP server running on port $portNumber")
                    else log.error("Could not start a HTTP server", it.cause())
                }
        }

    }

    private suspend fun indexHandler(context: RoutingContext) {
        val result = awaitResult<JsonArray> { dbService.fetchAllPages(it) }

        with(context) {
            put("title", "Wiki home")
            put("pages", result.list)
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
        val requestedPage = context.request().getParam("page")

        log.info("rendering requestedPage=$requestedPage")

        val result = awaitResult<JsonObject> { dbService.fetchPage(requestedPage, it) }
        val found = result.getBoolean("found")
        val rawContent = result.getString("rawContent", EMPTY_PAGE_MARKDOWN)
        val id = result.getInteger("id", -1)

        with(context) {
            put("title", requestedPage)
            put("id", id)
            put("newPage", if (!found) "yes" else "no")
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
        val id = request().getParam("id").toInt()
        val title = request().getParam("title")
        val markdown = request().getParam("markdown")
        val newPage = "yes" == request().getParam("newPage")

        log.info("update page id=$id and title=$title")

        val result = if (newPage) awaitResult<Void> { dbService.createPage(title, markdown, it) }
        else awaitResult { dbService.savePage(id, markdown, it) }

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
        val id = context.request().getParam("id").toInt()

        log.info("delete page with id=$id")

        awaitResult<Void> { dbService.deletePage(id, it) }

        context.response()
            .setStatusCode(303)
            .putHeader("Location", "/")
            .end()
    }

    private fun getPing(context: RoutingContext) = context
        .response()
        .setStatusCode(200)
        .end(
            JsonObject.mapFrom(
                object {
                    val ping = "pong"
                }
            ).toString())

    companion object {
        private val log = LoggerFactory.getLogger(HttpServerVerticle::class.java)
    }
}