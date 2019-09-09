package main

import (
	"fmt"
	"os"
	
	"gitlab.com/d5s/scp-load/cmd"
)

func main() {
	// defer profile.Start(profile.MemProfile, profile.ProfilePath(".")).Stop()
	
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
