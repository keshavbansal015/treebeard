package utils

import (
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func InitLogging(isLogEnabled bool, logPath string) {
	log.Debug().Msg("Initializing logging system.")
	if isLogEnabled {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
		log.Debug().Msg("Logging is enabled. Global level set to Debug.")
	} else {
		zerolog.SetGlobalLevel(zerolog.Disabled)
		log.Info().Msg("Logging is disabled.")
	}
	// Log the value of the logPath variable.
	log.Debug().Str("logPath", logPath).Msg("Checking log path.")
	if logPath == "" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
		log.Debug().Msg("No log path specified. Logging to standard error (console).")
	} else {
		logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Error().Err(err).Msg("Failed to open or create log file. Falling back to console logging.")
			// If there's an error, fall back to console logging to ensure logs are still captured.
			log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
			return // Exit the function to prevent further issues.
		}
		log.Logger = log.Output(logFile)
		log.Debug().Str("path", logPath).Msg("Logging to file.")
	}
	log.Debug().Msg("Logging initialization complete.")
}
