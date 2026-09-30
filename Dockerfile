# Development image: the rate limiter is a library, so nothing is compiled here.
# The container stays alive so you can exec into it and run tests, the example
# API and load tests with hey.
FROM golang:1.23-alpine AS dev

RUN apk add --no-cache git build-base curl \
    && go install github.com/rakyll/hey@v0.1.4

WORKDIR /app

ENV CGO_ENABLED=1

CMD ["tail", "-f", "/dev/null"]
