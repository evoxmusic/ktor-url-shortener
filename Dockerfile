FROM openjdk:8-jdk as builder
ENV APP_HOME=/usr/app/
WORKDIR $APP_HOME

COPY build.gradle.kts gradle.properties gradlew $APP_HOME
COPY gradle $APP_HOME/gradle

# downloads and caches dependencies in Docker image layer
# so that they don't have to be downloaded each time
RUN ./gradlew build || return 0

COPY . .
RUN ./gradlew shadowJar

FROM azul/zulu-openjdk-alpine:8-jre

COPY --from=builder /usr/app/build/libs/ktor-url-shortener-0.1.0-all.jar ktor-url-shortener.jar

EXPOSE 8080

CMD ["java", "-server", "-XX:+UnlockExperimentalVMOptions", "-XX:+UseCGroupMemoryLimitForHeap", "-XX:InitialRAMFraction=2", "-XX:MinRAMFraction=2", "-XX:MaxRAMFraction=2", "-XX:+UseG1GC", "-XX:MaxGCPauseMillis=100", "-XX:+UseStringDeduplication", "-jar", "ktor-url-shortener.jar"]
