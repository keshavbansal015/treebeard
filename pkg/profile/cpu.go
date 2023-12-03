package profile

import (
	"os"
	"os/signal"
	"runtime/pprof"

	"github.com/rs/zerolog/log"
)

type CPUProfile struct {
	file *os.File
}

func NewCPUProfile(fileName string) *CPUProfile {
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatal().Err(err).Msg("create cpu profile file failed")
	}

	return &CPUProfile{
		file: file,
	}
}

// Start starts the cpu profile
func (p *CPUProfile) Start() {
	pprof.StartCPUProfile(p.file)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		pprof.StopCPUProfile()
		p.file.Close()
		os.Exit(1)
	}()
}

// Stop stops the cpu profile and closes the file
func (p *CPUProfile) Stop() {
	pprof.StopCPUProfile()
	p.file.Close()
}
