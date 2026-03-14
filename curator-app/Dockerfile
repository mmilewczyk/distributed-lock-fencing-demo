FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY curator-app/target/curator-app-*.jar app.jar
ENTRYPOINT ["java", "--enable-preview", "-jar", "app.jar"]
