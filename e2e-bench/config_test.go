package e2ebench

import (
	"testing"
	"testing/synctest"
)

func TestConfigSelection(t *testing.T) {
	for _, tc := range []struct {
		name, path, scene, env, want string
	}{
		{name: "default", want: "config.json"},
		{name: "environment", env: "network", want: "config.network.json"},
		{name: "flag overrides environment", scene: "standard", env: "limited", want: "config.standard.json"},
		{name: "explicit path overrides both", path: "/tmp/custom.json", scene: "throttle", env: "limited", want: "/tmp/custom.json"},
		{name: "filename flag", scene: "config.throttle.json", want: "config.throttle.json"},
		{name: "filename environment", env: "config.limited.json", want: "config.limited.json"},
		{name: "custom scene", scene: "my-profile_2", want: "config.my-profile_2.json"},
		{name: "invalid unused environment", scene: "network", env: "../bad", want: "config.network.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveConfigPath(tc.path, tc.scene, tc.env)
			if err != nil || got != tc.want {
				t.Fatalf("path=%q err=%v, want %q", got, err, tc.want)
			}
		})
	}
	for _, scene := range []string{"../limited", "/tmp/network", "config..json", "config.network", "network.json", "network/extra", `network\extra`, " network"} {
		if _, err := resolveConfigPath("", scene, ""); err == nil {
			t.Errorf("accepted invalid scene %q", scene)
		}
	}
	path, err := resolveConfigPath("", "missing-preset", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil {
		t.Fatal("missing scene must fail instead of falling back to config.json")
	}
}

// Exercise each shipped file through a real stream with virtual time. Keep the
// payload small here; TestE2E measures the complete selected workload in real time.
func TestPresetScenes(t *testing.T) {
	seeds := make(map[int64]string)
	for _, scene := range []string{"standard", "throttle", "network", "limited"} {
		t.Run(scene, func(t *testing.T) {
			cfg, err := loadConfig("config." + scene + ".json")
			if err != nil {
				t.Fatal(err)
			}
			if previous, exists := seeds[cfg.Seed]; exists {
				t.Fatalf("seed %d shared with scene %s", cfg.Seed, previous)
			}
			seeds[cfg.Seed] = scene
			cfg.TotalBytes = 128 << 10
			synctest.Test(t, func(t *testing.T) {
				result, err := runTrial(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if result.Segments.VerifiedBytes != cfg.TotalBytes {
					t.Fatalf("verified %d bytes, want %d", result.Segments.VerifiedBytes, cfg.TotalBytes)
				}
				for _, stats := range result.Stats {
					if stats.QueueDrops != 0 || (cfg.PacketLossPercent == 0 && stats.RandomDrops != 0) {
						t.Fatalf("unexpected loss: %+v", stats)
					}
					if scene == "standard" && stats.ScheduledDelayNS != 0 {
						t.Fatal("standard scene added latency")
					}
				}
			})
		})
	}
}
