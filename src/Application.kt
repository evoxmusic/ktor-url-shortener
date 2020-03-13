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
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.`java-time`.datetime
import org.jetbrains.exposed.sql.transactions.transaction
import java.math.BigInteger
import java.security.MessageDigest
import java.time.LocalDateTime
import java.time.ZoneId
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
data class Response(val originalURL: String, val id: String, val stat: Stat = Stat()) {
    val shortURL: String = "${System.getenv("QOVERY_APPLICATION_API_HOST")}/$id"
}

object ResponseTable : Table("response") {
    val id = varchar("id", 32)
    val originalURL = varchar("original_url", 2048)
    override val primaryKey: PrimaryKey = PrimaryKey(id)
}

object ClickOverTimeTable : Table("click_over_time") {
    val id = integer("id").autoIncrement()
    val clickDate = datetime("click_date")
    val response = reference("request_id", onDelete = ReferenceOption.CASCADE, refColumn = ResponseTable.id)
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
        // drop tables
        // SchemaUtils.drop(ResponseTable, ClickOverTimeTable)
        // create tables if they do not exist
        SchemaUtils.createMissingTablesAndColumns(ResponseTable, ClickOverTimeTable)
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

    fun getResponseById(id: String): Response? {
        return transaction {
            ResponseTable.select { ResponseTable.id eq id }
                .limit(1)
                .map {
                    Response(
                        originalURL = it[ResponseTable.originalURL],
                        id = it[ResponseTable.id]
                    )
                }
        }.firstOrNull()
    }

    fun persistResponse(response: Response) {
        transaction {
            ResponseTable.insert {
                it[originalURL] = response.originalURL
                it[id] = response.id
            }
        }
    }

    fun getIdentifier(url: String, truncateLength: Int = 6): String {
        val id = url.encodeToID()

        val retrievedResponse = getResponseById(id)
        if (retrievedResponse != null && retrievedResponse.originalURL != url) {
            // collision spotted !
            return getIdentifier(url, truncateLength + 1)
        }

        return id
    }

    routing {
        get("/{id}") {
            val id = call.parameters["id"]
            val retrievedResponse = id?.let { getResponseById(it) }

            if (id.isNullOrBlank() || retrievedResponse == null) {
                return@get call.respondRedirect("https://www.google.com")
            }

            // add current date to the current response stats
            transaction {
                ClickOverTimeTable.insert {
                    it[clickDate] = LocalDateTime.now()
                    it[response] = retrievedResponse.id
                }
            }

            log.debug("redirect to: ${retrievedResponse.originalURL}")
            call.respondRedirect(retrievedResponse.originalURL)
        }

        get("/api/v1/url/{id}/stat") {
            val id = call.parameters["id"]
            val retrievedResponse = id?.let { getResponseById(it) }

            if (id.isNullOrBlank() || retrievedResponse == null) {
                return@get call.respond(HttpStatusCode.NoContent)
            }

            val dates: List<Date> = transaction {
                ClickOverTimeTable.select { ClickOverTimeTable.response eq id }
                    // convert LocalDateTime to Date
                    .map { Date.from(it[ClickOverTimeTable.clickDate].atZone(ZoneId.systemDefault()).toInstant()) }
            }

            retrievedResponse.stat.clicksOverTime.addAll(dates)

            call.respond(retrievedResponse.stat)
        }

        post("/api/v1/encode") {
            // Deserialize JSON body to Request object
            val request = call.receive<Request>()

            // find the Response object if it already exists
            val id = getIdentifier(request.url)

            // get Response from database
            val retrievedResponse = getResponseById(id)

            if (retrievedResponse != null) {
                // cache hit
                log.debug("cache hit $retrievedResponse")
                return@post call.respond(retrievedResponse)
            }

            // cache miss
            val response = request.toResponse()

            // persist data
            persistResponse(response)

            log.debug("cache miss $response")

            // Serialize Response object to JSON body
            call.respond(response)
        }
    }
}
