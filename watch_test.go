package viper

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestConcurrentHotReload(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "viper-watch-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, "config.json")

	// Write initial config
	initialConfig := map[string]string{"key": "initial"}
	initialBytes, _ := json.Marshal(initialConfig)
	if err := os.WriteFile(configPath, initialBytes, 0644); err != nil {
		t.Fatalf("failed to write initial config: %v", err)
	}

	v := New()
	v.SetConfigFile(configPath)
	if err := v.ReadInConfig(); err != nil {
		t.Fatalf("failed to read initial config: %v", err)
	}

	v.WatchConfig()

	// Wait a bit for watcher to start
	time.Sleep(100 * time.Millisecond)

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	// Concurrently write random valid configurations
	const numWriters = 5
	var finalVal string
	var finalValMu sync.Mutex

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for {
				select {
				case <-stopCh:
					return
				default:
					val := fmt.Sprintf("value-%d-%d", id, r.Intn(1000))
					cfg := map[string]string{"key": val}
					bytes, _ := json.Marshal(cfg)
					_ = os.WriteFile(configPath, bytes, 0644)

					finalValMu.Lock()
					finalVal = val
					finalValMu.Unlock()

					time.Sleep(time.Duration(r.Intn(10)+5) * time.Millisecond)
				}
			}
		}(i)
	}

	// Concurrently read values
	const numReaders = 5
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
					val := v.GetString("key")
					if val == "" {
						t.Errorf("read empty value for key")
					}
					time.Sleep(2 * time.Millisecond)
				}
			}
		}()
	}

	// Run the concurrent storm for 1 second
	time.Sleep(1 * time.Second)
	close(stopCh)
	wg.Wait()

	// Write one final known value and wait for it to settle
	expectedFinal := "final-settled-value"
	cfg := map[string]string{"key": expectedFinal}
	bytes, _ := json.Marshal(cfg)
	if err := os.WriteFile(configPath, bytes, 0644); err != nil {
		t.Fatalf("failed to write final config: %v", err)
	}

	// Wait for the debounce timer to fire and apply the change
	time.Sleep(250 * time.Millisecond)

	actualFinal := v.GetString("key")
	if actualFinal != expectedFinal {
		t.Errorf("expected final value %q, got %q", expectedFinal, actualFinal)
	}
}
