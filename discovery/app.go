// Package discovery runs standalone orchestrator discovery.
package discovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/j0sh/boa/pkg/boa"
	"github.com/livepeer/node/cmd/version"
	"github.com/livepeer/node/nodeconfig"
	"github.com/spf13/cobra"
)

type Params struct {
	ConfigFile string `name:"config" configfile:"optional-default" basepath:"source" default:"config.toml" boa:"noconfig" descr:"Configuration file; empty disables config discovery"`
	nodeconfig.Settings
	Listen             netip.AddrPort       `default:"127.0.0.1:8939" descr:"Public HTTP listener"`
	MetricsListen      netip.AddrPort       `default:"127.0.0.1:8940" descr:"Loopback metrics listener"`
	Nodes              []boa.Text[*url.URL] `optional:"true" descr:"Orchestrator or discovery node base URLs"`
	AdvertiseNodes     bool                 `default:"true" descr:"Advertise available discovery nodes"`
	AdvertiseRunners   bool                 `default:"true" descr:"Advertise collected runners"`
	RefreshInterval    time.Duration        `default:"5m" descr:"Discovery inventory refresh interval"`
	MaxFailedRefreshes int                  `default:"6" min:"0" descr:"Consecutive failed refreshes before eviction"`
	Grants             []string             `optional:"true" descr:"Private discovery IP[:port] or CIDR grants"`
	PrintConfig        bool                 `optional:"true" boa:"noconfig,noenv" descr:"Print TOML configuration"`
}

func Root(out, errOut io.Writer) *cobra.Command {
	params := new(Params)
	cmd := nodeconfig.Command("discovery", "offchain", boa.Cmd[Params]{
		Use: "livepeer-discovery", Short: "Standalone orchestrator discovery", Version: version.String(),
		Params: params, RejectUnknown: true,
		Args: cobra.NoArgs,
		RunFuncE: func(p *Params, cmd *cobra.Command, _ []string) error {
			if p.PrintConfig {
				app, err := NewServer(*p, slog.Default())
				if err != nil {
					return boa.NewUserInputError(err)
				}
				defer app.Close()
				return toml.NewEncoder(cmd.OutOrStdout()).Encode(map[string]any{
					"Network": &p.Network, "Listen": &p.Listen, "MetricsListen": &p.MetricsListen,
					"Nodes": &p.Nodes, "Grants": &p.Grants,
					"AdvertiseNodes": &p.AdvertiseNodes, "AdvertiseRunners": &p.AdvertiseRunners,
					"RefreshInterval": &p.RefreshInterval, "MaxFailedRefreshes": &p.MaxFailedRefreshes,
				})
			}
			return Serve(cmd.Context(), *p, cmd.ErrOrStderr())
		},
	}, &params.ConfigFile).ToCobra()
	cmd.SetOut(out)
	cmd.SetErr(errOut)
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.InitDefaultCompletionCmd()
	return cmd
}

func Serve(parent context.Context, p Params, logOut io.Writer) (result error) {
	logger := slog.New(slog.NewTextHandler(logOut, nil))
	app, err := NewServer(p, logger)
	if err != nil {
		return boa.NewUserInputError(err)
	}
	defer app.Close()
	listener, err := net.Listen("tcp", p.Listen.String())
	if err != nil {
		return fmt.Errorf("cannot start discovery HTTP listener on %s; choose an available --listen address: %w", p.Listen, err)
	}
	defer listener.Close()
	metricsListener, err := net.Listen("tcp", p.MetricsListen.String())
	if err != nil {
		return fmt.Errorf("cannot start discovery metrics listener on %s; choose an available --metrics-listen address: %w", p.MetricsListen, err)
	}
	defer metricsListener.Close()
	metrics := http.NewServeMux()
	metrics.Handle("GET /metrics", app.metrics.Handler())
	for path, body := range map[string]string{"/healthz": "ok\n", "/readyz": "ready\n"} {
		metrics.HandleFunc("GET "+path, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, body)
		})
	}
	ctx, stop := signal.NotifyContext(parent, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	server := func(handler http.Handler) *http.Server {
		return &http.Server{BaseContext: func(net.Listener) context.Context { return ctx }, Handler: handler,
			ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second,
			MaxHeaderBytes: 16 << 10, MaxHeaderValueCount: 128}
	}
	mainServer, metricsServer := server(app), server(metrics)
	errs := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Go(func() { errs <- mainServer.Serve(listener) })
	workers.Go(func() { errs <- metricsServer.Serve(metricsListener) })
	workers.Go(func() { app.Run(ctx) })
	logger.Info("discovery started", "listen", p.Listen, "metrics_listen", p.MetricsListen)
	select {
	case <-ctx.Done():
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			result = err
		}
	}
	stop()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, srv := range []*http.Server{mainServer, metricsServer} {
		if err := srv.Shutdown(shutdown); err != nil {
			result = errors.Join(result, err)
			_ = srv.Close()
		}
	}
	workers.Wait()
	return result
}
