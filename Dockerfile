# Build a static binary: the project is cgo-free (modernc.org/sqlite), so the
# runtime image needs nothing but the binary and CA certificates.
FROM golang:1.26-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/rc .

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata \
 && adduser -D -u 10001 rc

COPY --from=build /out/rc /usr/local/bin/rc

# State lives here: rc.db plus one log file per job.
RUN mkdir -p /var/lib/rc && chown rc:rc /var/lib/rc
VOLUME /var/lib/rc

USER rc
EXPOSE 8080

ENTRYPOINT ["rc"]
CMD ["serve", "--addr", "0.0.0.0:8080", "--data", "/var/lib/rc"]
