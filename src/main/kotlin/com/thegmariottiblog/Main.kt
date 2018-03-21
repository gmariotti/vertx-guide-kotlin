package com.thegmariottiblog

import com.thegmariottiblog.wiki.MainVerticle
import io.vertx.core.Vertx
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    val vertx = Vertx.vertx()
    vertx.deployVerticle(MainVerticle()) {
        if (it.succeeded()) println(APPLICATION_STARTED)
        else {
            println(APPLICATION_FAILED.format(it.cause()))
            exitProcess(-1)
        }
    }
}

private const val APPLICATION_STARTED = """
******************************
Application started
******************************
"""

private const val APPLICATION_FAILED = """
******************************
Could not start application:
%s
******************************
"""