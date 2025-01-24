// Copyright 2024 Google LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"crypto/tls"
	"log"
	"net"
	"net/http"
	//"context"
	"os"
	"time"

	extproc "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	//"google.golang.org/grpc/status"
	//  "google.golang.org/grpc/codes"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/peer"
	//"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Config holds the server configuration parameters.
type Config struct {
	Address              string
	InsecureAddress      string
	HealthCheckAddress   string
	CertFile             string
	KeyFile              string
	EnableInsecureServer bool
}

// loadConfig loads the server configuration from environment variables or uses defaults.
func loadConfig() Config {
	return Config{
		Address:              "0.0.0.0:8443",
		InsecureAddress:      "0.0.0.0:8181",
		HealthCheckAddress:   "0.0.0.0:8000",
		CertFile:             "extproc/ssl_creds/localhost.crt",
		KeyFile:              "extproc/ssl_creds/localhost.key",
		EnableInsecureServer: false,
	}
}

// CalloutServer represents a server that handles callouts.
type CalloutServer struct {
	Config Config
	Cert   tls.Certificate
}

var (

	// Define counters to track the number of requests processed by each method
	requestCountCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "grpc_requests_total",
			Help: "Total number of requests processed, by method",
		},
		[]string{"method"},
	)
)

func init() {
	prometheus.MustRegister(requestCountCounter)
}

// NewCalloutServer creates a new CalloutServer with the given configuration.
func NewCalloutServer(config Config) *CalloutServer {
	var cert tls.Certificate
	var err error

	if config.CertFile != "" && config.KeyFile != "" {
		cert, err = tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			log.Fatalf("Failed to load server certificate: %v", err)
		}
	}

	return &CalloutServer{
		Config: config,
		Cert:   cert,
	}
}

func StreamLoggingInterceptor(
	srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	// Log the method being called
	log.Printf("gRPC Stream - Method: %s", info.FullMethod)

	// Extract metadata (headers) from the incoming stream context
	md, _ := metadata.FromIncomingContext(stream.Context())
	log.Printf("gRPC Stream Headers: %v", md)

	// Log the peer (client) info
	p, ok := peer.FromContext(stream.Context())
	if ok {
		log.Printf("gRPC Peer: %s", p.Addr)
		log.Printf("gRPC Local Addr: %s", p.LocalAddr)
	}

	// Create a custom handler to intercept messages sent or received in the stream
	err := handler(srv, stream)
	if err != nil {
		log.Printf("gRPC Stream completed with error: %v", err)
	}

	return err
}

func (s *CalloutServer) StartGRPC(service extproc.ExternalProcessorServer) {
	// Start listening on the configured address
	lis, err := net.Listen("tcp", s.Config.Address)
	if err != nil {
		log.Fatalf("Failed to listen on %v: %v", s.Config.Address, err)
	}

	creds := credentials.NewServerTLSFromCert(&s.Cert)
	grpcServer := grpc.NewServer(
		grpc.Creds(creds),
		grpc.StreamInterceptor(StreamLoggingInterceptor), // Attach the stream interceptor here
	)

	// Register the gRPC service
	extproc.RegisterExternalProcessorServer(grpcServer, service)
	reflection.Register(grpcServer)
	log.Println("Reflection service registered.")

	// Start the gRPC server
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}

// StartHealthCheck starts a health check server.
func (s *CalloutServer) StartHealthCheck() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Addr:    s.Config.HealthCheckAddress,
		Handler: mux,
	}

	log.Fatal(server.ListenAndServe())
}

// StartInsecureGRPC starts the gRPC server without TLS.
func (s *CalloutServer) StartInsecureGRPC(service extproc.ExternalProcessorServer) {
	if !s.Config.EnableInsecureServer {
		return
	}
	lis, err := net.Listen("tcp", s.Config.InsecureAddress)
	if err != nil {
		log.Fatalf("Failed to listen on insecure port: %v", err)
	}
	grpcServer := grpc.NewServer()
	extproc.RegisterExternalProcessorServer(grpcServer, service)
	reflection.Register(grpcServer)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC on insecure port: %v", err)
	}
}

// RequestHeadersHandler handles request headers.
type RequestHeadersHandler func(*extproc.HttpHeaders) (*extproc.ProcessingResponse, error)

// ResponseHeadersHandler handles response headers.
type ResponseHeadersHandler func(*extproc.HttpHeaders) (*extproc.ProcessingResponse, error)

// RequestBodyHandler handles request bodies.
type RequestBodyHandler func(*extproc.HttpBody) (*extproc.ProcessingResponse, error)

// ResponseBodyHandler handles response bodies.
type ResponseBodyHandler func(*extproc.HttpBody) (*extproc.ProcessingResponse, error)

// RequestTrailersHandler handles request trailers.
type RequestTrailersHandler func(*extproc.HttpTrailers) (*extproc.ProcessingResponse, error)

// ResponseTrailersHandler handles response trailers.
type ResponseTrailersHandler func(*extproc.HttpTrailers) (*extproc.ProcessingResponse, error)

// HandlerRegistry registers various handlers.
type HandlerRegistry struct {
	RequestHeadersHandler   RequestHeadersHandler
	ResponseHeadersHandler  ResponseHeadersHandler
	RequestBodyHandler      RequestBodyHandler
	ResponseBodyHandler     ResponseBodyHandler
	RequestTrailersHandler  RequestTrailersHandler
	ResponseTrailersHandler ResponseTrailersHandler
}

// GRPCCalloutService implements the gRPC ExternalProcessorServer.
type GRPCCalloutService struct {
	extproc.UnimplementedExternalProcessorServer
	Handlers HandlerRegistry
}

func (s *GRPCCalloutService) Process(stream extproc.ExternalProcessor_ProcessServer) error {
	seededRand := "none"
	md, _ := metadata.FromIncomingContext(stream.Context())
	xRequestId := md["x-request-id"]
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		var response *extproc.ProcessingResponse
		var startTime time.Time // To hold start time for timing the request
		var methodName string

		switch {
		case req.GetRequestHeaders() != nil:
			if s.Handlers.RequestHeadersHandler != nil {
				methodName = "RequestHeaders"
				headers := req.GetRequestHeaders().GetHeaders().GetHeaders()
				for _, element := range headers {
					if element.Key == os.Getenv("REQUEST_HEADER_IDENTIFIER_KEY") && xRequestId != nil {
						seededRand = element.Key + "-" + string(element.RawValue[:]) + " " + xRequestId[0]
					}
				}
				log.Printf("[%v] Request Headers: %v", seededRand, req.GetRequestHeaders().Headers)
				startTime = time.Now() // Record start time
				response, err = s.Handlers.RequestHeadersHandler(req.GetRequestHeaders())
				log.Printf("[%v] RequestHeadersHandler took %v", seededRand, time.Since(startTime)) // Log the duration
			}
		case req.GetResponseHeaders() != nil:
			if s.Handlers.ResponseHeadersHandler != nil {
				methodName = "ResponseHeaders"
				log.Printf("[%v] Response Headers: %v", seededRand, req.GetResponseHeaders())
				startTime = time.Now()
				response, err = s.Handlers.ResponseHeadersHandler(req.GetResponseHeaders())
				log.Printf("[%v] ResponseHeadersHandler took %v", seededRand, time.Since(startTime))
			}
		case req.GetRequestBody() != nil:
			if s.Handlers.RequestBodyHandler != nil {
				methodName = "RequestBody"
				log.Printf("[%v] Request Body: %v", seededRand, req.GetRequestBody())
				startTime = time.Now()
				response, err = s.Handlers.RequestBodyHandler(req.GetRequestBody())
				log.Printf("[%v] RequestBodyHandler took %v", seededRand, time.Since(startTime))
			}
		case req.GetResponseBody() != nil:
			if s.Handlers.ResponseBodyHandler != nil {
				methodName = "ResponseBody"
				log.Printf("[%v] Response Body: %v", seededRand, req.GetResponseBody())
				startTime = time.Now()
				response, err = s.Handlers.ResponseBodyHandler(req.GetResponseBody())
				log.Printf("[%v] ResponseBodyHandler took %v", seededRand, time.Since(startTime))
			}
		case req.GetRequestTrailers() != nil:
			if s.Handlers.RequestTrailersHandler != nil {
				methodName = "RequestTrailers"
				log.Printf("[%v] Request Trailers: %v", seededRand, req.GetRequestTrailers())
				startTime = time.Now()
				response, err = s.Handlers.RequestTrailersHandler(req.GetRequestTrailers())
				log.Printf("[%v] RequestTrailersHandler took %v", seededRand, time.Since(startTime))
			}
		case req.GetResponseTrailers() != nil:
			if s.Handlers.ResponseTrailersHandler != nil {
				methodName = "ResponseTrailers"
				log.Printf("[%v] Response Trailers: %v", seededRand, req.GetResponseTrailers())
				startTime = time.Now()
				response, err = s.Handlers.ResponseTrailersHandler(req.GetResponseTrailers())
				log.Printf("[%v] ResponseTrailersHandler took %v", seededRand, time.Since(startTime))
			}
		}

		if err != nil {
			return err
		}
		// Record the metrics after processing the request
		if methodName != "" {
			// Increment the request count for the method
			requestCountCounter.WithLabelValues(methodName).Inc()
		}

		if response != nil {
			if err := stream.Send(response); err != nil {
				return err
			}
		}
	}
}

// HandleRequestHeaders handles request headers.
func (s *GRPCCalloutService) HandleRequestHeaders(headers *extproc.HttpHeaders) (*extproc.ProcessingResponse, error) {
	log.Printf("Handle request headers: %x", headers)
	return &extproc.ProcessingResponse{
		Response: &extproc.ProcessingResponse_RequestHeaders{
			RequestHeaders: &extproc.HeadersResponse{},
		},
	}, nil
}

// HandleResponseHeaders handles response headers.
func (s *GRPCCalloutService) HandleResponseHeaders(headers *extproc.HttpHeaders) (*extproc.ProcessingResponse, error) {
	log.Printf("Handle response headers: %x", headers)
	return &extproc.ProcessingResponse{
		Response: &extproc.ProcessingResponse_ResponseHeaders{
			ResponseHeaders: &extproc.HeadersResponse{},
		},
	}, nil
}

// HandleRequestBody handles request bodies.
func (s *GRPCCalloutService) HandleRequestBody(body *extproc.HttpBody) (*extproc.ProcessingResponse, error) {
	log.Printf("Handle request body: %x", body)
	return &extproc.ProcessingResponse{
		Response: &extproc.ProcessingResponse_RequestBody{
			RequestBody: &extproc.BodyResponse{},
		},
	}, nil
}

// HandleResponseBody handles response bodies.
func (s *GRPCCalloutService) HandleResponseBody(body *extproc.HttpBody) (*extproc.ProcessingResponse, error) {
	log.Printf("Handle response body: %x", body)
	return &extproc.ProcessingResponse{
		Response: &extproc.ProcessingResponse_ResponseBody{
			ResponseBody: &extproc.BodyResponse{},
		},
	}, nil
}

// HandleRequestTrailers handles request trailers.
func (s *GRPCCalloutService) HandleRequestTrailers(trailers *extproc.HttpTrailers) (*extproc.ProcessingResponse, error) {
	log.Printf("Handle request trailers: %x", trailers)
	return &extproc.ProcessingResponse{
		Response: &extproc.ProcessingResponse_RequestTrailers{
			RequestTrailers: &extproc.TrailersResponse{},
		},
	}, nil
}

// HandleResponseTrailers handles response trailers.
func (s *GRPCCalloutService) HandleResponseTrailers(trailers *extproc.HttpTrailers) (*extproc.ProcessingResponse, error) {
	log.Printf("Handle response trailers: %x", trailers)
	return &extproc.ProcessingResponse{
		Response: &extproc.ProcessingResponse_ResponseTrailers{
			ResponseTrailers: &extproc.TrailersResponse{},
		},
	}, nil
}
