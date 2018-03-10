package com.thegmariottiblog

import io.vertx.core.Vertx

fun main(args: Array<String>) {
    val vertx = Vertx.vertx()
    vertx.deployVerticle(MainVerticle()) {
        if (it.succeeded()) println(APPLICATION_STARTED)
        else println(APPLICATION_FAILED.format(it.cause().message))
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