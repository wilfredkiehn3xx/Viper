package viper

import (
	"fmt"
	"log"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// WatchConfig starts watching the config file for changes.
func WatchConfig() {
	v.WatchConfig()
}

func (v *Viper) WatchConfig() {
	initConfigFile()
	go func() {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Fatal(err)
		}
		defer watcher.Close()

		// we have to watch the interface directory to receive write events on symlinks
		configFile := filepath.Clean(v.configFile)
		configDir, _ := filepath.Split(configFile)
		realConfigFile, _ := filepath.EvalSymlinks(configFile)

		var (
			delay     = 100 * time.Millisecond
			timer     *time.Timer
			timerChan <-chan time.Time
			lastEvent fsnotify.Event
		)

		done := make(chan bool)
		go func() {
			for {
				select {
				case event, ok := <-watcher.Events:
					if !ok {
						return
					}
					currentConfigFile, _ := filepath.EvalSymlinks(configFile)
					// we only care about the config file with the following cases:
					// 1. write or rename event on the config file itself
					// 2. write or rename event on a symlink to the config file
					if (filepath.Clean(event.Name) == configFile &&
						(event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Rename == fsnotify.Rename)) ||
						(currentConfigFile != "" && currentConfigFile != realConfigFile) {
						realConfigFile = currentConfigFile
						lastEvent = event

						if timer != nil {
							if !timer.Stop() {
								select {
								case <-timerChan:
								default:
								}
							}
						}
						timer = time.NewTimer(delay)
						timerChan = timer.C
					}
				case <-timerChan:
					timerChan = nil
					timer = nil

					// Read config into a temporary Viper instance
					tmp := New()
					v.mu.Lock()
					tmp.configFile = v.configFile
					tmp.configType = v.configType
					tmp.keyDelim = v.keyDelim
					tmp.fs = v.fs
					v.mu.Unlock()

					err := tmp.ReadInConfig()
					if err != nil {
						log.Printf("error reading config file: %v", err)
						continue
					}

					if len(tmp.config) == 0 {
						log.Printf("warning: config file is empty, keeping existing configuration")
						continue
					}

					v.mu.Lock()
					v.config = tmp.config
					v.mu.Unlock()

					if v.onConfigChange != nil {
						v.onConfigChange(lastEvent)
					}
				case err, ok := <-watcher.Errors:
					if !ok {
						return
					}
					log.Println("error:", err)
				}
			}
		}()

		watcher.Add(configDir)
		<-done
	}()
}
