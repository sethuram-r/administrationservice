FROM adoptopenjdk/openjdk13:jre-13.0.2_8-alpine
LABEL maintainer = sethuram
WORKDIR /usr/src/administration
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} app.jar
EXPOSE 8085
ENTRYPOINT ["java","-jar","app.jar"]