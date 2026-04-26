package cli

import "github.com/spf13/cobra"

func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "metacritic-harvester",
		Short:        "Collect public Metacritic list data into SQLite",
		SilenceUsage: true,
	}

	rootCmd.AddCommand(newCrawlCommand())
	rootCmd.AddCommand(newDetailCommand())
	rootCmd.AddCommand(newLatestCommand())
	rootCmd.AddCommand(newReviewCommand())
	rootCmd.AddCommand(newServeCommand())
	return rootCmd
}
