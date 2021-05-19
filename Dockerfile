FROM bitnami/spark:latest
COPY . ./
CMD ls
CMD spark-submit myApp.jar