FROM golang:1.23-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY internal ./internal
COPY cmd/ingest ./cmd/ingest
RUN CGO_ENABLED=0 go build -o /ingest ./cmd/ingest

FROM gcr.io/distroless/base-debian12
COPY --from=build /ingest /ingest
ENTRYPOINT ["/ingest"]