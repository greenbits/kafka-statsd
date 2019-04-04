FROM golang:1.12 AS build

WORKDIR /app
COPY go.mod go.sum main.go ./
RUN go build -v

FROM gcr.io/distroless/base
COPY --from=build /app/kafka-statsd kafka-statsd
ENTRYPOINT ["/kafka-statsd"]
CMD ["--help"]
