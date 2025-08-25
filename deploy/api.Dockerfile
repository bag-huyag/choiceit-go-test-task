FROM golang:1.23-alpine AS build
WORKDIR /app
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY internal ./internal
COPY cmd/api ./cmd/api
RUN CGO_ENABLED=0 go build -o /api ./cmd/api

FROM gcr.io/distroless/base-debian12
COPY --from=build /api /api
EXPOSE 8080
ENTRYPOINT ["/api"]