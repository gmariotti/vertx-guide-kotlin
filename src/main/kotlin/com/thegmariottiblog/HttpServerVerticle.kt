package com.thegmariottiblog

import EMPTY_PAGE_MARKDOWN
import com.github.rjeschke.txtmark.Processor
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.http.HttpServer
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
    private lateinit var wikiDbQueue: String

    override suspend fun start() {
        wikiDbQueue = config.getString(CONFIG_WIKIDB_QUEUE, WIKIDB_QUEUE)

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
        val options = DeliveryOptions().addHeader("action", "all-pages")
        val result = awaitResult<Message<JsonObject>> { vertx.eventBus().send(wikiDbQueue, JsonObject(), options, it) }
            .body()

        with(context) {
            put("title", "Wiki home")
            put("pages", result.getJsonArray("pages").list)
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
        val request = JsonObject().put("page", requestedPage)
        val options = DeliveryOptions().addHeader("action", "get-page")

        log.info("rendering requestedPage=$requestedPage")

        val result = awaitResult<Message<JsonObject>> { vertx.eventBus().send(wikiDbQueue, request, options, it) }
            .body()
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
        val id = request().getParam("id")
        val title = request().getParam("title")
        val markdown = request().getParam("markdown")
        val newPage = "yes" == request().getParam("newPage")

        log.info("update page id=$id and title=$title")

        val request = JsonObject()
            .put("id", id)
            .put("title", title)
            .put("markdown", markdown)

        val options = DeliveryOptions()
            .addHeader("action", if (newPage) "create-page" else "save-page")

        awaitResult<Message<Void>> { vertx().eventBus().send(wikiDbQueue, request, options, it) }

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

        val request = JsonObject().put("id", id)
        val options = DeliveryOptions().addHeader("action", "delete-page")

        awaitResult<Message<Void>> { vertx.eventBus().send(wikiDbQueue, request, options, it) }

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