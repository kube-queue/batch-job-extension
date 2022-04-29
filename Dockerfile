FROM golang:1.17.8-alpine as build
RUN apk add make git
ADD . /go/src/github.com/kube-queue/job-extension
WORKDIR /go/src/github.com/kube-queue/job-extension
RUN make

FROM alpine:3.12
COPY --from=build /go/src/github.com/kube-queue/job-extension/bin/job-extension /usr/bin/job-extension
RUN chmod +x /usr/bin/job-extension
ENTRYPOINT ["/usr/bin/job-extension"]
