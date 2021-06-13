#FROM openjdk:8
#FROM nginx:alpine
FROM alpine:3.7
COPY ./target/scala-2.13/bullynode.jar /app/src/app.jar
WORKDIR /app/src
RUN apk add openjdk8
ENTRYPOINT ["java", "-jar","app.jar"]