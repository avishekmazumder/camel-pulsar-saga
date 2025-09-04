# syntax=docker/dockerfile:1
############################################
# Build stage
############################################
FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /app

# Copy only pom first to leverage Docker layer caching for dependencies
COPY pom.xml .
RUN mvn -B -q -DskipTests=true dependency:go-offline

# Now copy sources and build the JAR
COPY src ./src
RUN mvn -B -q -DskipTests=true package

############################################
# Runtime stage
############################################
FROM eclipse-temurin:17-jre-alpine AS runtime

# Create a non-root user
RUN addgroup -S spring && adduser -S spring -G spring

WORKDIR /app

# Copy the fat JAR produced by Spring Boot
COPY --from=build /app/target/orchestrator-0.0.1-SNAPSHOT.jar app.jar

# Expose the port configured in application.properties
EXPOSE 8080

# Optional: allow JVM tuning and Spring profile via env vars
ENV JAVA_OPTS=""     SPRING_PROFILES_ACTIVE=dev

USER spring

# Use sh -c so JAVA_OPTS can be expanded
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
