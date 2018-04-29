package com.thegmariottiblog.wiki.http

import ch.vorburger.mariadb4j.DB
import com.thegmariottiblog.extensions.fold
import com.thegmariottiblog.wiki.CONFIG_WIKIDB_JDBC_URL
import com.thegmariottiblog.wiki.database.WikiDatabaseVerticle
import com.thegmariottiblog.wiki.database.WikiDatabaseVerticleTest
import io.vertx.core.AsyncResult
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.ext.web.codec.BodyCodec
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.json.get
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.awaitResult
import kotlinx.coroutines.experimental.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.platform.commons.logging.LoggerFactory

@ExtendWith(VertxExtension::class)
class ApiTest {

    companion object {
        private val db by lazy { DB.newEmbeddedDB(0) }
        private val log = LoggerFactory.getLogger(WikiDatabaseVerticleTest::class.java)

        @BeforeAll @JvmStatic
        fun `initialize database`() {
            db.start()
        }

        @AfterAll @JvmStatic
        fun `stop database`() {
            db.stop()
        }
    }

    private lateinit var webClient: WebClient

    @BeforeEach
    fun prepare(vertx: Vertx, vertxTestContext: VertxTestContext) {
        db.createDB("db")

        log.info { "...deploy initialization..." }

        val config = JsonObject()
            .put(CONFIG_WIKIDB_JDBC_URL, "jdbc:mariadb://localhost:${db.configuration.port}/db?user=root")
        vertx.deployVerticle(
            WikiDatabaseVerticle(),
            DeploymentOptions().setConfig(config),
            vertxTestContext.succeeding()
        )
        vertx.deployVerticle(HttpServerVerticle(), vertxTestContext.succeeding())
        webClient = WebClient.create(vertx, WebClientOptions().apply {
            defaultHost = "localhost"
            defaultPort = 8080
        })
        vertxTestContext.completeNow()

        log.info { "...deploy completed..." }
    }

    @AfterEach
    fun cleanup(vertx: Vertx, vertxTestContext: VertxTestContext) = runBlocking {
        log.info { "...cleanup started..." }

        JDBCClient.createShared(vertx, json {
            obj(
                "url" to "jdbc:mariadb://localhost:${db.configuration.port}?user=root",
                "driver_class" to "org.mariadb.jdbc.Driver"
            )
        }).apply {
            awaitResult<ResultSet> { this.query("DROP DATABASE IF EXISTS db", it) }
        }.close()
        vertx.close(vertxTestContext.succeeding())
        vertxTestContext.completeNow()

        log.info { "...cleanup completed..." }
    }

    @Test
    fun `play with api`(vertxTestContext: VertxTestContext) = runBlocking {
        val page = JsonObject().put("name", "sample").put("markdown", "# A page")
        awaitEvent<AsyncResult<HttpResponse<JsonObject>>> {
            webClient.post("/api/pages")
                .`as`(BodyCodec.jsonObject())
                .sendJsonObject(page, it)
        }.fold(
            onSuccess = {
                vertxTestContext.verify { assertTrue(it.body().getBoolean("success")) }
            },
            onFailure = { vertxTestContext.failNow(it) }
        )

        awaitEvent<AsyncResult<HttpResponse<JsonObject>>> {
            webClient.get("/api/pages")
                .`as`(BodyCodec.jsonObject())
                .send(it)
        }.fold(
            onSuccess = {
                val response = it.body()
                vertxTestContext.verify {
                    assertTrue(response.getBoolean("success"))
                    val pages = response.getJsonArray("pages")
                    assertEquals(1, pages.size())
                    assertEquals(1, pages.get<JsonObject>(0).getInteger("id"))
                    assertEquals("sample", pages.get<JsonObject>(0).getString("name"))
                }
            },
            onFailure = { vertxTestContext.failNow(it) }
        )

        awaitEvent<AsyncResult<HttpResponse<JsonObject>>> {
            webClient.put("/api/pages/1")
                .`as`(BodyCodec.jsonObject())
                .sendJsonObject(JsonObject().put("name", "Oh yeah").put("markdown", "Oh yeah!"), it)
        }.fold(
            onSuccess = {
                vertxTestContext.verify { assertTrue(it.body().getBoolean("success")) }
            },
            onFailure = { vertxTestContext.failNow(it) }
        )

        awaitEvent<AsyncResult<HttpResponse<JsonObject>>> {
            webClient.delete("/api/pages/1")
                .`as`(BodyCodec.jsonObject())
                .send(it)
        }.fold(
            onSuccess = {
                vertxTestContext.verify { assertTrue(it.body().getBoolean("success")) }
            },
            onFailure = { vertxTestContext.failNow(it) }
        )

        vertxTestContext.completeNow()
    }
}