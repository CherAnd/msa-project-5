FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

COPY build/libs/*.jar app.jar
ADD https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/download/v2.23.0/opentelemetry-javaagent.jar /app/opentelemetry-javaagent.jar

ENV OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://jaeger:4317
ENV OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=grpc

ENTRYPOINT ["java", "-Dotel.exporter.otlp.traces.endpoint=http://jaeger:4317","-Dotel.exporter.otlp.traces.protocol=grpc", "-javaagent:/app/opentelemetry-javaagent.jar","-jar", "app.jar"]
EXPOSE 8080
