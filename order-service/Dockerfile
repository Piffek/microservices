# Multi-stage build — buduje serwis wewnątrz kontenera.
# Etap 1: "build" — kompilacja Maven (wymaga JDK + Maven)
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# Najpierw kopiujemy pomy (lepsze cache warstw — pobieranie zależności
# jest re-uruchamiane tylko gdy zmieni się pom.xml, nie kod źródłowy)
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY order-service/pom.xml order-service/pom.xml
COPY inventory-service/pom.xml inventory-service/pom.xml
COPY notification-service/pom.xml notification-service/pom.xml

# Pobierz zależności (z cache jeśli pom.xml się nie zmienił)
RUN mvn dependency:go-offline -pl common,order-service -am -q

# Teraz kopiujemy kod źródłowy
COPY common/src common/src
COPY order-service/src order-service/src

# Budujemy TYLKO common i order-service (-pl = project list, -am = also make dependencies)
RUN mvn -pl common,order-service -am package -DskipTests -q

# Etap 2: "runtime" — minimalne środowisko uruchomieniowe (tylko JRE, bez JDK/Maven)
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Kopiujemy JAR z etapu build
COPY --from=build /app/order-service/target/*.jar app.jar

# Healthcheck — Docker sprawdza czy serwis żyje co 30 sekund
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD wget -qO- http://localhost:8081/actuator/health || exit 1

EXPOSE 8081

ENTRYPOINT ["java", "-jar", "app.jar"]
