FROM alpine:latest

COPY kafka_exporter.linux /bin/kafka_exporter

EXPOSE 9308
ENTRYPOINT [ "/bin/kafka_exporter" ]
