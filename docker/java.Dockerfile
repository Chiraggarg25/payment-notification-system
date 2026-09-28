# syntax=docker/dockerfile:1
# One Dockerfile for every Spring Boot module:  --build-arg MODULE=<module-name>
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY common common
COPY platform-service platform-service
COPY notification-service notification-service
COPY sweeper-service sweeper-service
COPY mock-vendor-api mock-vendor-api
ARG MODULE
# The cache mount keeps ~/.m2 between builds so dependencies are downloaded only once.
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    mvn -B -q -pl ${MODULE} -am -DskipTests package

FROM eclipse-temurin:21-jre
ARG MODULE
# curl is only used by the docker-compose healthcheck (GET /actuator/health)
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /workspace/${MODULE}/target/${MODULE}.jar app.jar
# heap = 75% of the container memory limit (mem_limit in docker-compose.yml)
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
