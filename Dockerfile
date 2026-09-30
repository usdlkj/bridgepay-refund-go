FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /bridgepay-refund-go ./cmd/api

FROM alpine:3.23
RUN apk add --no-cache ca-certificates curl && addgroup -S bridgepay && adduser -S -G bridgepay bridgepay
WORKDIR /usr/src/app
COPY --from=build /bridgepay-refund-go /usr/src/app/bridgepay-refund-go
USER bridgepay
EXPOSE 4000
ENTRYPOINT ["/usr/src/app/bridgepay-refund-go"]
