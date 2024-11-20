// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/benchmark/delay/helloworld"
	"google.golang.org/grpc/benchmark/latency"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"math"
	"net"
	"os"
	"strings"
	"time"
)

const port = "50000"

var (
	defaultWindowSize        int32 = 65535                         // from gRPC
	defaultInitialWindowSize       = defaultWindowSize * 32        // 2MB
	maximumWindowSize              = defaultInitialWindowSize * 32 // 64MB
)

var serverKeepalive = keepalive.ServerParameters{
	// Send periodic pings on the connection when there is no other traffic.
	Time: 2 * time.Second,
	// Close the connection if either a keepalive ping doesn't receive a response
	// within the timeout, or a TCP send doesn't receive a TCP ack within the
	// timeout (enforced by the OS via TCP_USER_TIMEOUT).
	Timeout: 4 * time.Second,
}

var serverEnforcement = keepalive.EnforcementPolicy{
	MinTime:             time.Nanosecond,
	PermitWithoutStream: true,
}

func main() {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	defer lis.Close()

	// Add latency to the network.
	// lis = latency.Longhaul.Listener(lis)
	// lis = latency.LAN.Listener(lis)
	// nw := latency.Longhaul
	nw := latency.Network{
		Kbps:    1000 * 1024,
		Latency: time.Millisecond * 250,
		MTU:     1500,
	}
	lis = nw.Listener(lis)

	grpcOpts := []grpc.ServerOption{
		// The limiting factor for lowering the max message size is the fact
		// that a single large kv can be sent over the network in one message.
		// Our maximum kv size is unlimited, so we need this to be very large.
		//
		// TODO(peter,tamird): need tests before lowering.
		grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		// Adjust the stream and connection window sizes. The gRPC defaults are too
		// low for high latency connections.
		grpc.InitialWindowSize(defaultInitialWindowSize),
		grpc.InitialConnWindowSize(maximumWindowSize),
		// The default number of concurrent streams/requests on a client connection
		// is 100, while the server is unlimited. The client setting can only be
		// controlled by adjusting the server value. Set a very large value for the
		// server value so that we have no fixed limit on the number of concurrent
		// streams/requests on either the client or server.
		grpc.MaxConcurrentStreams(math.MaxInt32),
		grpc.KeepaliveParams(serverKeepalive),
		grpc.KeepaliveEnforcementPolicy(serverEnforcement),
	}
	s := grpc.NewServer(grpcOpts...)
	pb.RegisterGreeterServer(s, &server{})

	fmt.Println("starting server")

	go func() {
		err = s.Serve(lis)
		if err != nil {
			panic(err)
		}
	}()

	time.Sleep(1 * time.Second)
	fmt.Println("connecting to server")

	// Connect to the server.
	// conn, err := grpc.Dial(lis.Addr().String(), []grpc.DialOption{}...)
	opts := []grpc.DialOption{}
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		// return nw.ContextDialer((internal.NetDialerWithTCPKeepalive().DialContext))(ctx, "tcp", lis.Addr().String())
		return nw.ContextDialer((&net.Dialer{}).DialContext)(ctx, "tcp", lis.Addr().String())
	}))
	conn, err := grpc.Dial(lis.Addr().String(), opts...)
	if err != nil {
		panic(err)
	}

	c := pb.NewGreeterClient(conn)

	benchmark(c, 0)
	benchmark(c, 20*time.Millisecond)
	benchmark(c, 5*time.Second)

	os.Exit(0)
}

type server struct {
	pb.UnimplementedGreeterServer
}

var bigReply = strings.Repeat("abcdefghijklmnop", 50_000)

func (s *server) SayHello(_ context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	if in.Name == "small" {
		return &pb.HelloReply{Message: "hello"}, nil
	} else {
		return &pb.HelloReply{Message: bigReply}, nil
	}
}

const iters = 5

func benchmark(c pb.GreeterClient, interval time.Duration) {
	fmt.Printf("Interval: %v...", interval)
	smallReq := &pb.HelloRequest{
		Name: "small",
	}
	bigReq := &pb.HelloRequest{
		Name: strings.Repeat("asdfasldh", 20_000),
	}

	var latency time.Duration
	for i := 0; i < iters; i++ {
		for i := 0; i < 3; i++ {
			_, err := c.SayHello(context.Background(), smallReq)
			if err != nil {
				panic(err)
			}
		}

		t0 := time.Now()
		_, err := c.SayHello(context.Background(), bigReq)
		if err != nil {
			panic(err)
		}
		l := time.Since(t0)
		latency += l

		fmt.Printf(" %v,", l)
		time.Sleep(interval)
	}

	fmt.Printf(" Average Latency: %v\n", latency/iters)
}
