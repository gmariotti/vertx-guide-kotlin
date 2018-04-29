package com.thegmariottiblog.extensions

import io.vertx.core.AsyncResult
import io.vertx.core.http.HttpServerResponse
import io.vertx.ext.web.Route
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.experimental.launch

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

inline fun <T> AsyncResult<T>.fold(onSuccess: (T) -> Unit, onFailure: (Throwable) -> Unit) {
    if (this.succeeded()) onSuccess(this.result())
    else onFailure(this.cause())
}

fun HttpServerResponse.addJsonHeader(): HttpServerResponse = putHeader("Content-Type", "application/json")