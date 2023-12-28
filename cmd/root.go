/*
Copyright © 2023 Jean-Baptiste Thomas <jboothomas@gmail.com>
This file is part of CLI application p53.
*/
package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	pS3Version string = "0.1.15"
)

var (
	//flag variables
	fCfgFile     string
	fEndpointUrl string
	fVerbose     bool
	fDebug       bool
	fTrace       bool
	fNoVerifySSL bool
	fOutput      string
	fProfile     string
	fRegion      string
	fVersion     bool

	//env variables
	ePATH string

	//characters to use for prefix creation
	characters = []string{" ", "!", "&", "'", "(", ")", "+", ",", "-", ".", "/", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", ":", ";", "=", "?", "@",
		"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
		"_", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
		"*", "$"}

	//max keys per page to return from s3 API call
	maxKeys int64 = 1000
	//max sempahore concurrent thread counter for s3api calls
	maxSemaphore int = 256
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "pS3",
	Short: "A CLI for fast S3 operations on buckets with high counts of objects",
	Long: `A CLI tool to replace certain S3 operations with faster versions when 
used on buckets with millions of objects.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		if fVersion {
			fmt.Println("pS3 version", pS3Version)
			return
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	//rootCmd.PersistentFlags().StringVar(&fCfgFile, "config", "", "config file (default is $HOME/.p53.yaml)")

	rootCmd.PersistentFlags().BoolVar(&fVerbose, "verbose", false, "Turn on verbose output.")

	//Debug includes verbose
	rootCmd.PersistentFlags().BoolVar(&fDebug, "debug", false, "Turn on debug output.")
	rootCmd.PersistentFlags().MarkHidden("debug")
	//Trace includes debug and verbose
	rootCmd.PersistentFlags().BoolVar(&fTrace, "trace", false, "Turn on trace output.")
	rootCmd.PersistentFlags().MarkHidden("trace")

	rootCmd.PersistentFlags().StringVar(&fEndpointUrl, "endpoint-url", "", "Override command’s default URL with the given URL")

	rootCmd.PersistentFlags().BoolVar(&fNoVerifySSL, "no-verify-ssl", false, "Override SSL certificate verification")
	//"By default, p53 CLI uses SSL when communicating with S3 services. For each SSL connection, the p53 CLI will verify SSL certificates. This option overrides the default behavior of verifying SSL certificates.")

	rootCmd.PersistentFlags().StringVar(&fOutput, "output", "text", "The formatting style for command output: json, text.")

	rootCmd.PersistentFlags().StringVar(&fProfile, "profile", "", "Use a specific profile from your credential file")

	rootCmd.PersistentFlags().StringVar(&fRegion, "region", "", "The region to use. Overrides config/env settings.")

	rootCmd.Flags().BoolVar(&fVersion, "version", false, "Display version information.")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	//rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	viper.BindEnv("PATH")
	ePATH = viper.Get("PATH").(string)
	//fmt.Printf("debug: %s\n", ePATH)

	if fCfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(fCfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".pS3" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".pS3")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Fprintln(os.Stderr, "Using config file:", viper.ConfigFileUsed())
	}
}
