package main

import (
	"context"
	"encoding/json"
	pb "externalscaler-sample/externalscaler"
	"fmt"
	"io/ioutil"
	"log"
	"net"

	// "log"
	// "net"
	"net/http"
	"time"
	
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	url_pkg "net/url"
	"strconv"

	"google.golang.org/grpc/reflection"

)

type ExternalScaler struct{}

type USGSResponse struct {
	Features []USGSFeature `json:"features"`
}

type USGSFeature struct {
	Properties USGSProperties `json:"properties"`
}

type USGSProperties struct {
	Mag float64 `json:"mag"`
}

type prometheusScaler struct {
	metadata   *prometheusMetadata
	httpClient *http.Client
}

const (
	promServerAddress = "serverAddress"
	promMetricName    = "metricName"
	promQuery         = "query"
	promThreshold     = "threshold"
)

type prometheusMetadata struct {
	serverAddress string
	metricName    string
	query         string
	threshold     int
	currentValue int
}

type promQueryResult struct {
	Status string `json:"status"`
	Data   struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric struct {
			} `json:"metric"`
			Value []interface{} `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

var prometheusLog = logf.Log.WithName("prometheus_scaler")

func (s *prometheusScaler) IsActive(ctx context.Context, scaledObject *pb.ScaledObjectRef) (*pb.IsActiveResponse, error) {
	val, err := s.ExecutePromQuery()
	if err != nil {
		prometheusLog.Error(err, "error executing prometheus query")
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.IsActiveResponse{
		Result: val > 0,
	}, nil

	//return val > 0, nil
}

func (s *prometheusScaler) GetMetricSpec(context.Context, *pb.ScaledObjectRef) (*pb.GetMetricSpecResponse, error) {
	return &pb.GetMetricSpecResponse{
		MetricSpecs: []*pb.MetricSpec{{
			MetricName: "Prometheus Query Target",
			TargetSize: 3,
		}},
	}, nil
}

func (s *prometheusScaler) GetMetrics(_ context.Context, metricRequest *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	val, err := s.ExecutePromQuery()
	if err != nil {
		prometheusLog.Error(err, "error executing prometheus query")
		return &pb.GetMetricsResponse{},err 
		
	}
	
	return &pb.GetMetricsResponse{
		MetricValues: []*pb.MetricValue{{
			MetricName: "Prometheus Metric",
			MetricValue: int64(val),
			//Timestamp:  metav1.Now(),
		}},
	}, nil
}

func (s *prometheusScaler) ExecutePromQuery() (float64, error) {
	t := time.Now().UTC().Format(time.RFC3339)
	queryEscaped := url_pkg.QueryEscape(s.metadata.query)
	url := fmt.Sprintf("%s/api/v1/query?query=%s&time=%s", s.metadata.serverAddress, queryEscaped, t)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return -1, err
	}
	r, err := s.httpClient.Do(req)
	if err != nil {
		return -1, err
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return -1, err
	}
	r.Body.Close()

	if !(r.StatusCode >= 200 && r.StatusCode <= 299) {
		return -1, fmt.Errorf("prometheus query api returned error. status: %d response: %s", r.StatusCode, string(b))
	}

	var result promQueryResult
	err = json.Unmarshal(b, &result)
	if err != nil {
		return -1, err
	}

	var v float64 = -1

	// allow for zero element or single element result sets
	if len(result.Data.Result) == 0 {
		return 0, nil
	} else if len(result.Data.Result) > 1 {
		return -1, fmt.Errorf("prometheus query %s returned multiple elements", s.metadata.query)
	}

	val := result.Data.Result[0].Value[1]
	if val != nil {
		s := val.(string)
		v, err = strconv.ParseFloat(s, 64)
		if err != nil {
			prometheusLog.Error(err, "Error converting prometheus value", "prometheus_value", s)
			return -1, err
		}
	}

	return v, nil
}

func (s *prometheusScaler) StreamIsActive(scaledObject *pb.ScaledObjectRef, epsServer pb.ExternalScaler_StreamIsActiveServer) error {

	for {
		select {
		case <-epsServer.Context().Done():
			// call cancelled
			return nil
		case <-time.Tick(time.Hour * 1):
			val, err := s.ExecutePromQuery()
			if err != nil {
				// log error
			} else if val > 0 {
				err = epsServer.Send(&pb.IsActiveResponse{
					Result: true,
				})
			}
		}
	}
}

func main() {
	grpcServer := grpc.NewServer()
	reflection.Register(grpcServer)
	lis, _ := net.Listen("tcp", ":6000")
	pb.RegisterExternalScalerServer(grpcServer, &prometheusScaler{})

	fmt.Println("listenting on :6000")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
