FROM maven:3.9.6-eclipse-temurin-11 AS builder

WORKDIR /build

COPY pom.xml spotless.eclipseformat.xml ./
COPY src ./src
COPY docker/maven-settings.xml /root/.m2/settings.xml

RUN mvn -q -DskipTests clean package

FROM flink:1.17.2-scala_2.12-java11

ENV APP_HOME=/opt/app
ENV APP_JAR_NAME=flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar
ENV JAR_PATH=/opt/app/lib/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar

WORKDIR /opt/app

RUN mkdir -p /opt/app/bin /opt/app/conf /opt/app/data /opt/app/lib /opt/app/samples /opt/app/scripts

COPY --from=builder /build/target/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar /opt/app/lib/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar
COPY docker/conf/flink-job.image.conf /opt/app/conf/flink-job.conf
COPY docker/conf/bootstrap.image.conf /opt/app/conf/bootstrap.conf
COPY docker/scripts/bootstrap-kaggle.sh /opt/app/scripts/bootstrap-kaggle.sh
COPY docker/scripts/run-e2e.sh /opt/app/scripts/run-e2e.sh
COPY docker/scripts/submit-flink-job.sh /opt/app/scripts/submit-flink-job.sh
COPY docker/scripts/wait-for-port.sh /opt/app/scripts/wait-for-port.sh

RUN chmod +x /opt/app/scripts/*.sh
