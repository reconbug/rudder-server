# syntax=docker/dockerfile:1
FROM golang:1.20.1-alpine3.17 AS builder

RUN mkdir workdir
WORKDIR /workdir

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN go build -o worker ./event-schema-2/service/cmd/

FROM alpine:3.16

COPY --from=builder workdir/service .

CMD ["./service"]
