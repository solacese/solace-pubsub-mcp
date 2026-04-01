# Build stage
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src

RUN apt-get update && apt-get install -y maven && rm -rf /var/lib/apt/lists/*
RUN mvn package -DskipTests -q

# Runtime stage
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

RUN groupadd -r mcp && useradd -r -g mcp -s /bin/false mcp
COPY --from=build /app/target/solace-pubsub-mcp-0.1.0.jar /app/solace-pubsub-mcp.jar

USER mcp

ENTRYPOINT ["java", "-jar", "/app/solace-pubsub-mcp.jar"]
