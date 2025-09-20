package cmd

import (
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/chronomq/chronomq/pkg/chronomq"
	"github.com/chronomq/chronomq/pkg/persistence"
	"github.com/chronomq/chronomq/pkg/protocol"
)

var (
	// defaultAddrs holds the various endpoints we need to configure
	defaultAddrs = &addrs{
		rpcAddr:   ":11301",
		grpcAddr:  ":9999",
		statsAddr: ":8125",
	}
	// appCfg - wires in the application and configuration
	appCfg = &config{
		addrs: defaultAddrs,
	}
	serverCmd = &cobra.Command{
		Use:   "server",
		Short: "Run server with disk-based job storage",
		Long:  `Stores job data on disk and rebuilds indices in memory on startup.`,
		Run: func(cmd *cobra.Command, args []string) {
			log.Info().Int("PID", os.Getpid()).Msg("Starting Server")
			startApp(appCfg)
			log.Info().Msg("Shutdown ok")
		},
	}
)

// config wires in the application and configuration
type config struct {
	addrs       *addrs
	jobsDir     string        // Directory for job storage
	restore     bool          // If true, hub will attempt restore on startup
	spokeSpan   time.Duration // Spoke duration
}

func init() {
	serverCmd.PersistentFlags().DurationVarP(&appCfg.spokeSpan, "spokeSpan", "S", time.Second*10, "Spoke span (golang duration string format)")
	serverCmd.PersistentFlags().BoolVarP(&appCfg.restore, "restore", "r", false, "Restore existing data if possible from store")
	dataDir, _ := os.Getwd()
	serverCmd.Flags().StringVar(&appCfg.jobsDir, "jobs-dir", dataDir, "Directory for job storage")

	rootCmd.AddCommand(serverCmd)
}

func startApp(cfg *config) {
	// More Aggressive GC
	if os.Getenv("GOGC") == "" {
		log.Info().Msg("Applying default GC tuning")
		debug.SetGCPercent(5)
	} else {
		log.Info().Str("GCPercent", os.Getenv("GOGC")).Msg("Using custom GC tuning")
	}
	go func() {
		err := http.ListenAndServe(":6060", nil)
		if err != nil {
			log.Error().Err(err).Msg("pprof server has stopped")
		}
	}()

	log.Info().Msg("Starting Chronomq")

	// Create disk storage for job data using simple file-based approach
	jobsDir := filepath.Join(cfg.jobsDir, "jobs")
	diskStore := persistence.NewDiskJobStore(jobsDir)

	opts := &chronomq.HubOpts{
		AttemptRestore: cfg.restore,
		SpokeSpan:      cfg.spokeSpan,
		DiskStore:      diskStore,
		MaxCFSize:      chronomq.DefaultMaxCFSize,
	}

	h := chronomq.NewHub(opts)

	// Rebuild indices from disk if restore flag is set
	if cfg.restore {
		if err := h.RebuildIndicesFromDisk(); err != nil {
			log.Fatal().Err(err).Msg("Failed to rebuild indices from disk")
		}
	}

	var rpcSRV io.Closer
	wg := sync.WaitGroup{}
	go func() {
		rpcSRV, _ = protocol.ServeRPC(h, cfg.addrs.rpcAddr)
	}()
	var grpcSrv *grpc.Server
	var lis net.Listener
	go func() {
		// Start the grpc server
		grpcSrv, lis, _ = protocol.ServeGRPC(h, cfg.addrs.grpcAddr)
	}()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGUSR1, syscall.SIGTERM)

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-sigc
		log.Info().Msg("Stopping rpc protocol server")
		rpcSRV.Close()
		log.Info().Msg("Stopping rpc protocol server - Done")

		log.Info().Msg("Shutting down gRPC server...")
		grpcSrv.GracefulStop()
		lis.Close()
		log.Info().Msg("gRPC server stopped.")
		h.Stop(true)
	}()

	wg.Wait()
}
