package dfs

import (
	"log"
	"os"

	"io/ioutil"
)

var logger *log.Logger

func init() {
	logger = log.New(ioutil.Discard, "dfs: ", log.Lshortfile)
}

// SetLogging to change log output
func SetLogging(val *log.Logger) {
	logger = val
}

func Verbose() {
	logger = log.New(os.Stdout, "dfs: ", log.Lshortfile)
}
