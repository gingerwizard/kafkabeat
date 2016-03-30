package main

import (
	"os"

	"github.com/elastic/beats/libbeat/beat"

	"github.com/gingerwizard/kafkabeat/beater"
)

func main() {
	err := beat.Run("kafkabeat", "", beater.New())
	if err != nil {
		os.Exit(1)
	}
}
