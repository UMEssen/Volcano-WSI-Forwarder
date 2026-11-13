# Volcano WSI Forwarder: whole-slide images from the scanner share into the research PACS (STOW-RS).
FROM sbtscala/scala-sbt:eclipse-temurin-21.0.13_9_1.13.1_3.3.9 AS build
WORKDIR /build
COPY build.sbt .
COPY project/build.properties project/plugins.sbt project/
RUN sbt -batch update
COPY src src
RUN sbt -batch assembly

FROM eclipse-temurin:21-jre-noble
RUN useradd --system --uid 10001 --no-create-home forwarder
COPY --from=build /build/target/scala-3.3.6/volcano-wsi-forwarder.jar /app/volcano-wsi-forwarder.jar
USER 10001
EXPOSE 8080
# Heap for headers, Kafka and HTTP buffers only: instances are streamed, never held.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/volcano-wsi-forwarder.jar"]
