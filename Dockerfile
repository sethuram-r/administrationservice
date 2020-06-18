FROM adoptopenjdk/openjdk13:jre-13.0.2_8-alpine
LABEL maintainer = sethuram
WORKDIR /usr/src/administration
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} app.jar
EXPOSE 8085
ENV CORE_SERVER_HOSTNAME=localhost CORE_SERVER_HOSTNAME=8084 JDBC_URL=jdbc:postgresql://localhost:5432/postgres DATABASE_USERNAME=postgres DATABASE_PASSWORD=postgres
ENTRYPOINT ["java","-jar","app.jar"]