package com.thegmariottiblog.wiki.database

import ch.vorburger.mariadb4j.DB
import com.thegmariottiblog.wiki.CONFIG_WIKIDB_JDBC_URL
import com.thegmariottiblog.wiki.CONFIG_WIKIDB_QUEUE
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.ResultSet
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import io.vertx.kotlin.core.DeploymentOptions
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
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
class WikiDatabaseVerticleTest {
    private lateinit var service: WikiDatabaseService

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

    @BeforeEach
    fun `deploy verticle`(vertx: Vertx, vertxTestContext: VertxTestContext) {
        db.createDB("db")

        log.info { "...deploy initialization..." }
        val config = JsonObject()
            .put(CONFIG_WIKIDB_JDBC_URL, "jdbc:mariadb://localhost:${db.configuration.port}/db?user=root")
        val options = DeploymentOptions().setConfig(config)
        vertx.deployVerticle(
            WikiDatabaseVerticle(),
            options,
            vertxTestContext.succeeding {
                service = createProxy(vertx, CONFIG_WIKIDB_QUEUE)
                vertxTestContext.completeNow()
            }
        )
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
        log.info { "...cleanup completed..." }
        vertxTestContext.completeNow()
    }

    @Test
    fun `crud operations`(vertxTestContext: VertxTestContext) {
        runBlocking {
            awaitResult<Void> { service.createPage("Test", "Some content", it) }
            val fetchPageRes = awaitResult<JsonObject> { service.fetchPage("Test", it) }
            vertxTestContext.verify {
                assertTrue(fetchPageRes.getBoolean("found"))
                assertTrue(fetchPageRes.containsKey("id"))
                assertEquals("Some content", fetchPageRes.getString("rawContent"))
            }

            awaitResult<Void> { service.savePage(fetchPageRes.getInteger("id"), "Yo!", it) }
            val fetchAllRes = awaitResult<JsonArray> { service.fetchAllPages(it) }
            vertxTestContext.verify {
                assertEquals(1, fetchAllRes.size())
            }

            val fetchYoPageRes = awaitResult<JsonObject> { service.fetchPage("Test", it) }
            vertxTestContext.verify {
                assertEquals("Yo!", fetchYoPageRes.getString("rawContent"))
            }

            awaitResult<Void> { service.deletePage(fetchPageRes.getInteger("id"), it) }
            val noPages = awaitResult<JsonArray> { service.fetchAllPages(it) }
            vertxTestContext.verify {
                assertTrue(noPages.isEmpty)
            }
        }
        vertxTestContext.completeNow()
    }
}