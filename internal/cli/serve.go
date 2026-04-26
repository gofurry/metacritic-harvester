package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/GoFurry/metacritic-harvester/internal/config"
	serveapp "github.com/GoFurry/metacritic-harvester/internal/serve"
)

func newServeCommand() *cobra.Command {
	var (
		addr           string
		dbPath         string
		fullStack      bool
		enableWrite    bool
		baseURL        string
		backendBaseURL string
	)

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run a local HTTP server for querying data and triggering crawl tasks",
		RunE: func(cmd *cobra.Command, _ []string) error {
			srv := serveapp.NewServer(serveapp.Config{
				Addr:           addr,
				DBPath:         dbPath,
				FullStack:      fullStack,
				EnableWrite:    enableWrite,
				BaseURL:        baseURL,
				BackendBaseURL: backendBaseURL,
			})

			fmt.Fprintf(
				cmd.ErrOrStderr(),
				"serve starting: addr=%s db=%s full_stack=%t enable_write=%t\n",
				addr,
				dbPath,
				fullStack,
				enableWrite,
			)
			return srv.Run(cmd.Context())
		},
	}

	cmd.Flags().StringVar(&addr, "addr", "127.0.0.1:8080", "Listen address for the HTTP server")
	cmd.Flags().StringVar(&dbPath, "db", "output/metacritic.db", "SQLite database path")
	cmd.Flags().BoolVar(&fullStack, "full-stack", false, "Serve the embedded control-panel frontend")
	cmd.Flags().BoolVar(&enableWrite, "enable-write", false, "Enable task-triggering write endpoints (local requests only)")
	cmd.Flags().StringVar(&baseURL, "base-url", config.DefaultBaseURL, "Base Metacritic site URL")
	cmd.Flags().StringVar(&backendBaseURL, "backend-base-url", config.DefaultBackendBaseURL, "Base backend API URL")

	return cmd
}
