# One image with every binary; docker-compose picks the command per service.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.22
RUN adduser -D -u 10001 app
COPY --from=build /out/ /usr/local/bin/
USER app
ENTRYPOINT []
