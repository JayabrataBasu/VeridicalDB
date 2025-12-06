FROM golang:1.23-alpine AS build
WORKDIR /src
COPY . .
RUN apk add --no-cache git
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-s -w" -o /out/veridicaldb ./cmd/veridicaldb

FROM scratch
COPY --from=build /out/veridicaldb /veridicaldb
EXPOSE 5432
ENTRYPOINT ["/veridicaldb"]
