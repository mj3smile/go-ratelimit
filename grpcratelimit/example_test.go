package grpcratelimit_test

import (
	"log"
	"net"

	ratelimit "github.com/mj3smile/go-ratelimit"
	"github.com/mj3smile/go-ratelimit/grpcratelimit"
	"google.golang.org/grpc"
)

func Example() {
	limiter, err := ratelimit.New(ratelimit.PerSecond(50))
	if err != nil {
		log.Fatal(err)
	}

	server := grpc.NewServer(
		grpc.UnaryInterceptor(grpcratelimit.UnaryServerInterceptor(limiter)),
		grpc.StreamInterceptor(grpcratelimit.StreamServerInterceptor(limiter)),
	)
	// Register your services on server, then serve.

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(server.Serve(lis))
}
