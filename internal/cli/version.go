package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	Version   = "dev"
	CommitSHA = "none"
	BuildDate = "unknown"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("scalesync %s (commit: %s, built: %s)\n", Version, CommitSHA, BuildDate)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
