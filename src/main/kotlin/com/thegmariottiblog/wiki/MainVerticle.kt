package com.thegmariottiblog.wiki

import com.thegmariottiblog.wiki.database.WikiDatabaseVerticle
import com.thegmariottiblog.wiki.http.HttpServerVerticle
import io.vertx.core.DeploymentOptions
import io.vertx.ext.jdbc.JDBCClient
import io.vertx.ext.sql.ResultSet
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitResult

class MainVerticle : CoroutineVerticle() {
    override suspend fun start() {
        initializeDatabase()
        awaitResult<String> { vertx.deployVerticle(WikiDatabaseVerticle(), it) }
        awaitResult<String> {
            vertx.deployVerticle(
                HttpServerVerticle::class.java,
                DeploymentOptions().setInstances(2),
                it
            )
        }
    }

    private suspend fun initializeDatabase() {
        JDBCClient.createShared(vertx, json {
            obj(
                "url" to "jdbc:mariadb://localhost:3306?user=root",
                "driver_class" to config.getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, "org.mariadb.jdbc.Driver")
            )
        }).apply {
            awaitResult<ResultSet> { this.query("CREATE DATABASE IF NOT EXISTS db", it) }
        }.close()
    }
}
