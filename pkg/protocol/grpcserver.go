package protocol

import (
	"context"
	"net"
	"time"

	pb "github.com/chronomq/chronomq/api/grpc/chronomq"
	"github.com/chronomq/chronomq/internal/monitor"
	"github.com/chronomq/chronomq/pkg/chronomq"
	"github.com/rs/zerolog/log"

	"google.golang.org/grpc"
)

type GRPCServer struct {
	pb.UnimplementedChronoMQServer
	hub *chronomq.Hub
}

func newGRPCServer(hub *chronomq.Hub) *GRPCServer {
	return &GRPCServer{hub: hub}
}

func (s *GRPCServer) PutWithID(ctx context.Context, req *pb.PutWithIDRequest) (*pb.PutWithIDResponse, error) {
	j := req.Job
	var job *chronomq.Job
	if j.Id == "" {
		job = chronomq.NewJobAutoID(time.Now().Add(time.Millisecond*time.Duration(j.DelayMillis)), j.Body)
	} else {
		job = chronomq.NewJob(j.Id, time.Now().Add(time.Millisecond*time.Duration(j.DelayMillis)), j.Body)
	}
	monitor.GetMemMonitor().Increment(job)
	err := s.hub.AddJobLocked(job)
	return &pb.PutWithIDResponse{Id: job.ID()}, err
}

func (s *GRPCServer) Cancel(ctx context.Context, req *pb.CancelRequest) (*pb.CancelResponse, error) {
	job, err := s.hub.CancelJobLocked(req.Id)
	if job != nil {
		monitor.GetMemMonitor().Decrement(job)
	}
	return &pb.CancelResponse{}, err
}

func (s *GRPCServer) Next(ctx context.Context, req *pb.NextRequest) (*pb.NextResponse, error) {
	timeout := time.Millisecond * time.Duration(req.TimeoutMillis)
	j, err := s.hub.NextLocked()
	if err != nil {
		log.Error().Err(err).Msg("Error retrieving job from indexed hub")
		return nil, err
	}
	if j != nil {
		monitor.GetMemMonitor().Decrement(j)
		return &pb.NextResponse{Job: toPBJob(j)}, nil
	}
	if timeout == 0 {
		return nil, ErrTimeout
	}
	waitUntil := time.Now().Add(timeout)
	for time.Now().Before(waitUntil) {
		j, err := s.hub.NextLocked()
		if err != nil {
			log.Error().Err(err).Msg("Error retrieving job from indexed hub during wait")
			return nil, err
		}
		if j != nil {
			monitor.GetMemMonitor().Decrement(j)
			return &pb.NextResponse{Job: toPBJob(j)}, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil, ErrTimeout
}

func (s *GRPCServer) Ping(ctx context.Context, _ *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{Pong: "pong"}, nil
}

func (s *GRPCServer) InspectN(ctx context.Context, req *pb.InspectNRequest) (*pb.InspectNResponse, error) {
	// Hub returns job indices, not full jobs
	// For inspection, we'll return job metadata from indices
	indices := s.hub.GetNJobIndices(int(req.N))
	var resp []*pb.Job
	for idx := range indices {
		// Create a lightweight job representation from the index
		resp = append(resp, &pb.Job{
			Id:          idx.ID(),
			Body:        nil, // Don't load full body for inspection
			DelayMillis: int64(idx.TriggerAt().Sub(time.Now()).Milliseconds()),
		})
	}
	return &pb.InspectNResponse{Jobs: resp}, nil
}

func toPBJob(j *chronomq.Job) *pb.Job {
	return &pb.Job{
		Id:          j.ID(),
		Body:        j.Body(),
		DelayMillis: int64(j.TriggerAt().Sub(time.Now()).Milliseconds()),
	}
}

// ServeGRPC starts serving indexed hub over gRPC
func ServeGRPC(hub *chronomq.Hub, addr string) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}

	s := grpc.NewServer()
	pb.RegisterChronoMQServer(s, newGRPCServer(hub))
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Error().Err(err).Msg("gRPC serve error")
			return
		}
	}()

	return s, lis, nil
}
