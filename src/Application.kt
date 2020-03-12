package com.qovery.oss

import com.fasterxml.jackson.databind.PropertyNamingStrategy
import com.fasterxml.jackson.databind.SerializationFeature
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.ktor.application.Application
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.application.log
import io.ktor.features.ContentNegotiation
import io.ktor.http.HttpStatusCode
import io.ktor.jackson.jackson
import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.response.respondRedirect
import io.ktor.routing.get
import io.ktor.routing.post
import io.ktor.routing.routing
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.ReferenceOption
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.`java-time`.datetime
import org.jetbrains.exposed.sql.transactions.transaction
import java.math.BigInteger
import java.security.MessageDigest
import java.util.*

fun main(args: Array<String>): Unit = io.ktor.server.netty.EngineMain.main(args)

// String extension
fun String.encodeToID(truncateLength: Int = 6): String {
    // hash String with MD5
    val hashBytes = MessageDigest.getInstance("MD5").digest(this.toByteArray(Charsets.UTF_8))
    // transform to human readable MD5 String
    val hashString = String.format("%032x", BigInteger(1, hashBytes))
    // truncate MD5 String
    val truncatedHashString = hashString.take(truncateLength)
    // return id
    return truncatedHashString
}

// Request object
data class Request(val url: String) {
    fun toResponse(): Response = Response(url, url.encodeToID())
}

data class Stat(val clicksOverTime: MutableList<Date> = mutableListOf())

// Response object
data class Response(val originalURL: String, private val id: String, val stat: Stat = Stat()) {
    val shortURL: String = "${System.getenv("QOVERY_APPLICATION_API_HOST")}/$id"
}

object RequestTable : Table("request") {
    val id = varchar("id", 32)
    val originalURL = varchar("original_url", 2048)
    override val primaryKey: PrimaryKey = PrimaryKey(id)
}

object ClickOverTimeTable : Table("click_over_time") {
    val id = integer("id").autoIncrement()
    val clickDate = datetime("click_date")
    val request = reference("request_id", onDelete = ReferenceOption.CASCADE, refColumn = RequestTable.id)
    override val primaryKey: PrimaryKey = PrimaryKey(id)
}

fun initDatabase() {
    val config = HikariConfig().apply {
        jdbcUrl = "jdbc:${System.getenv("QOVERY_DATABASE_MY_PQL_DB_CONNECTION_URI_WITHOUT_CREDENTIALS")}"
        username = System.getenv("QOVERY_DATABASE_MY_PQL_DB_USERNAME")
        password = System.getenv("QOVERY_DATABASE_MY_PQL_DB_PASSWORD")
        driverClassName = "org.postgresql.Driver"
    }

    Database.connect(HikariDataSource(config))

    transaction {
        // create tables if they do not exist
        SchemaUtils.createMissingTablesAndColumns(RequestTable, ClickOverTimeTable)
    }
}

@kotlin.jvm.JvmOverloads
fun Application.module(testing: Boolean = false) {
    initDatabase()

    install(ContentNegotiation) {
        jackson {
            enable(SerializationFeature.INDENT_OUTPUT)
            propertyNamingStrategy = PropertyNamingStrategy.SNAKE_CASE
            // add this line to return Date object as ISO8601 format
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        }
    }

    // Hash Table Response object by id
    val responseByID = mutableMapOf<String, Response>()

    fun getShortURL(url: String, truncateLength: Int = 6): String {
        val id = url.encodeToID()

        val retrievedResponse = responseByID[id]
        if (retrievedResponse != null && retrievedResponse.originalURL != url) {
            // collision spotted !
            return getShortURL(url, truncateLength + 1)
        }

        return id
    }

    routing {
        get("/{id}") {
            val id = call.parameters["id"]
            val retrievedResponse = id?.let { responseByID[it] }

            if (id.isNullOrBlank() || retrievedResponse == null) {
                return@get call.respondRedirect("https://www.google.com")
            }

            // add current date to the current response stats
            retrievedResponse.stat.clicksOverTime.add(Date())

            log.debug("redirect to: ${retrievedResponse.originalURL}")
            call.respondRedirect(retrievedResponse.originalURL)
        }

        get("/api/v1/url/{id}/stat") {
            val id = call.parameters["id"]
            val retrievedResponse = id?.let { responseByID[it] }

            if (id.isNullOrBlank() || retrievedResponse == null) {
                return@get call.respond(HttpStatusCode.NoContent)
            }

            call.respond(retrievedResponse.stat)
        }

        post("/api/v1/encode") {
            // Deserialize JSON body to Request object
            val request = call.receive<Request>()

            // find the Response object if it already exists
            val shortURL = getShortURL(request.url)
            val retrievedResponse = responseByID[shortURL]
            if (retrievedResponse != null) {
                // cache hit
                log.debug("cache hit $retrievedResponse")
                return@post call.respond(retrievedResponse)
            }

            // cache miss
            val response = request.toResponse()
            responseByID[shortURL] = response
            log.debug("cache miss $response")

            // Serialize Response object to JSON body
            call.respond(response)
        }
    }
}
