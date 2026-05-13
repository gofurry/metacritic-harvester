package cli

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/gofurry/metacritic-harvester/internal/app"
	"github.com/gofurry/metacritic-harvester/internal/config"
)

func newCrawlScheduleCommand() *cobra.Command {
	return newCrawlScheduleCommandWithRunner(func(ctx context.Context, filePath string) error {
		scheduleFile, err := config.LoadScheduleFile(filePath)
		if err != nil {
			return err
		}

		return app.NewScheduleService(config.DefaultBaseURL).Run(ctx, scheduleFile)
	})
}

func newCrawlScheduleCommandWithRunner(runner func(context.Context, string) error) *cobra.Command {
	var filePath string

	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Run scheduled batch jobs from a YAML file",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()

			return runner(ctx, filePath)
		},
	}

	cmd.Flags().StringVar(&filePath, "file", "", "YAML schedule file path")
	_ = cmd.MarkFlagRequired("file")
	cmd.SetHelpFunc(func(command *cobra.Command, args []string) {
		command.Parent().HelpFunc()(command, args)
		_, _ = command.OutOrStdout().Write([]byte("\nThis command runs in the foreground until interrupted.\n"))
		_, _ = command.OutOrStdout().Write([]byte("Cron expressions support 5 fields and optional seconds.\n"))
	})

	return cmd
}
