package protocol_test

import (
	"context"
	"fmt"
	"net"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/chronomq/chronomq/api/grpc/chronomq"
	"github.com/chronomq/chronomq/pkg/chronomq"
	"github.com/chronomq/chronomq/pkg/persistence"
	"github.com/chronomq/chronomq/pkg/protocol"
)

var _ = Describe("Test grpc protocol:", func() {
	defer GinkgoRecover()
	var port = 7800
	var client pb.ChronoMQClient
	var conn *grpc.ClientConn

	var srv *grpc.Server
	var lis net.Listener
	var h *chronomq.Hub

	BeforeEach(func(done Done) {
		defer close(done)
		store, err := persistence.InMemStorage()
		Expect(err).NotTo(HaveOccurred())
		var opts = chronomq.HubOpts{
			AttemptRestore: false,
			Persister:      persistence.NewJournalPersister(store),
			SpokeSpan:      time.Second * 5}
		h = chronomq.NewHub(&opts)
		srv, lis, err = protocol.ServeGRPC(h, ":0")
		Expect(err).NotTo(HaveOccurred())
		port++

		// Connect to the server
		Eventually(func() error {
			var opts []grpc.DialOption
			opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
			conn, err = grpc.NewClient(lis.Addr().String(), opts...)
			if err != nil {
				return err
			}
			client = pb.NewChronoMQClient(conn)
			return nil
		}, "1s").Should(BeNil())
	}, 1.0)

	AfterEach(func(done Done) {
		defer close(done)
		if conn != nil {
			conn.Close()
		}

		if srv != nil {
			srv.Stop()
		}

		srv = nil
		h = nil
		client = nil
	})

	It("pings grpc server", func(done Done) {
		defer close(done)
		defer GinkgoRecover()
		_, err := client.Ping(context.Background(), &pb.PingRequest{})
		Expect(err).NotTo(HaveOccurred())
	}, 0.1)

	It("Puts a job and then reads it", func(done Done) {
		defer close(done)
		defer GinkgoRecover()

		hw := "Hello world"
		job1 := &pb.Job{
			Body:        []byte(hw),
			DelayMillis: 1,
		}
		resp1, err := client.PutWithID(context.Background(), &pb.PutWithIDRequest{Job: job1})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp1.Id).ToNot(BeEmpty())

		job2 := &pb.Job{
			Body:        []byte(hw),
			DelayMillis: 1,
		}
		resp2, err := client.PutWithID(context.Background(), &pb.PutWithIDRequest{Job: job2})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp2.Id).ToNot(BeEmpty())

		job3 := &pb.Job{
			Body:        []byte(hw),
			DelayMillis: 1,
		}
		resp3, err := client.PutWithID(context.Background(), &pb.PutWithIDRequest{Job: job3})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp3.Id).ToNot(BeEmpty())

		nextResp, err := client.Next(context.Background(), &pb.NextRequest{TimeoutMillis: int64(time.Minute.Milliseconds())})
		Expect(err).NotTo(HaveOccurred())
		Expect(nextResp.Job.Id).To(Equal(resp1.Id))
		Expect(string(nextResp.Job.Body)).To(Equal(hw))
	}, 20)

	It("Puts a job with an id and then reads it", func(done Done) {
		defer close(done)
		defer GinkgoRecover()

		_, err := client.Ping(context.Background(), &pb.PingRequest{})
		Expect(err).NotTo(HaveOccurred())

		hw := "Hello world"
		job := &pb.Job{
			Id:          "foo",
			Body:        []byte(hw),
			DelayMillis: 1,
		}
		_, err = client.PutWithID(context.Background(), &pb.PutWithIDRequest{Job: job})
		ExpectNoErr(err)

		// We can inspect without consuming too
		inspectResp, err := client.InspectN(context.Background(), &pb.InspectNRequest{N: 2})
		Expect(err).To(BeNil())
		Expect(len(inspectResp.Jobs)).To(Equal(1))
		Expect(inspectResp.Jobs[0].Id).To(Equal("foo"))
		Expect(inspectResp.Jobs[0].Body).To(Equal([]byte(hw)))

		nextResp, err := client.Next(context.Background(), &pb.NextRequest{TimeoutMillis: int64(time.Minute.Milliseconds())})
		Expect(err).NotTo(HaveOccurred())
		Expect(nextResp.Job.Id).To(Equal("foo"))
		Expect(string(nextResp.Job.Body)).To(Equal(hw))
	}, 20)

	It("Puts multiple jobs with ids and then inpects them", func(done Done) {
		defer close(done)
		defer GinkgoRecover()

		_, err := client.Ping(context.Background(), &pb.PingRequest{})
		Expect(err).NotTo(HaveOccurred())

		n := 10
		hw := "Hello world"
		for i := 0; i < n; i++ {
			job := &pb.Job{
				Id:          fmt.Sprintf("foo%d", i),
				Body:        []byte(hw),
				DelayMillis: 1,
			}
			_, err := client.PutWithID(context.Background(), &pb.PutWithIDRequest{Job: job})
			ExpectNoErr(err)
		}

		// InspectN < n
		inspectN := 5
		inspectResp, err := client.InspectN(context.Background(), &pb.InspectNRequest{N: int32(inspectN)})
		Expect(err).To(BeNil())
		Expect(len(inspectResp.Jobs)).To(Equal(inspectN))
		for i := 0; i < inspectN; i++ {
			Expect(inspectResp.Jobs[i].Id).To(Equal(fmt.Sprintf("foo%d", i)))
			Expect(inspectResp.Jobs[i].Body).To(Equal([]byte(hw)))
		}

		// InspectN == n
		inspectN = n
		inspectResp, err = client.InspectN(context.Background(), &pb.InspectNRequest{N: int32(inspectN)})
		Expect(err).To(BeNil())
		Expect(len(inspectResp.Jobs)).To(Equal(inspectN))
		for i := 0; i < inspectN; i++ {
			Expect(inspectResp.Jobs[i].Id).To(Equal(fmt.Sprintf("foo%d", i)))
			Expect(inspectResp.Jobs[i].Body).To(Equal([]byte(hw)))
		}

		// InspectN > n
		inspectN = n + 3
		inspectResp, err = client.InspectN(context.Background(), &pb.InspectNRequest{N: int32(inspectN)})
		Expect(err).To(BeNil())
		Expect(len(inspectResp.Jobs)).To(Equal(n))
		for i := 0; i < n; i++ {
			Expect(inspectResp.Jobs[i].Id).To(Equal(fmt.Sprintf("foo%d", i)))
			Expect(inspectResp.Jobs[i].Body).To(Equal([]byte(hw)))
		}

		// Read them all
		for i := 0; i < n; i++ {
			nextResp, err := client.Next(context.Background(), &pb.NextRequest{TimeoutMillis: int64(time.Minute.Milliseconds())})
			Expect(err).NotTo(HaveOccurred())
			Expect(nextResp.Job.Id).To(Equal(fmt.Sprintf("foo%d", i)))
			Expect(string(nextResp.Job.Body)).To(Equal(hw))
		}
	}, 20)

	It("Puts a job and then deletes it", func(done Done) {
		defer close(done)
		defer GinkgoRecover()
		hw := "Hello world"
		job := &pb.Job{
			Body:        []byte(hw),
			DelayMillis: 1,
		}
		resp, err := client.PutWithID(context.Background(), &pb.PutWithIDRequest{Job: job})
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Id).ToNot(BeEmpty())

		//delete
		_, err = client.Cancel(context.Background(), &pb.CancelRequest{Id: resp.Id})
		ExpectNoErr(err)
	})
})
