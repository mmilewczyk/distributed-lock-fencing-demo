FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY redisson-app/target/redisson-app-*.jar app.jar
ENTRYPOINT ["java", "--enable-preview", "-jar", "app.jar"]
