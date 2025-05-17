# Этап сборки
FROM eclipse-temurin:21-jdk as builder

WORKDIR /app

COPY . .

# Ставим права и билдим
RUN chmod +x mvnw && ./mvnw clean package -DskipTests

# Финальный образ
FROM eclipse-temurin:21-jdk

WORKDIR /app

# JVM флаг, если нужен
ENV JAVA_OPTS="--add-opens java.base/java.lang=ALL-UNNAMED"

# Копируем JAR из target, потому что Maven кладёт туда
COPY --from=builder /app/target/*.jar app.jar

# Запуск приложения
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
