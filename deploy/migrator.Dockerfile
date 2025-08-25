FROM golang:1.23-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY internal ./internal
COPY cmd/migrator ./cmd/migrator
RUN CGO_ENABLED=0 go build -o /migrator ./cmd/migrator


FROM gcr.io/distroless/base-debian12
COPY --from=build /migrator /migrator
ENTRYPOINT ["/migrator"]