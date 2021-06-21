#FROM openjdk:8
#FROM nginx:alpine
FROM alpine:3.7
RUN apk add openjdk8
COPY ./target/scala-2.13/bullynode.jar /app/src/app.jar
WORKDIR /app/src
ENTRYPOINT ["java", "-jar","app.jar"]