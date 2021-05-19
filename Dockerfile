FROM bitnami/spark:latest
COPY . ./
CMD ls
CMD spark-submit target/scala-2.12/myApp.jar