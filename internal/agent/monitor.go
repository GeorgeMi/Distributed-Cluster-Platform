package agent

import (
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

func GetMetrics() (totalCPU, usedCPU float64, totalRAM, usedRAM int64) {
	totalCPU = float64(runtime.NumCPU())

	// Approximate CPU usage from load average
	loadAvg := getLoadAvg()
	usedCPU = loadAvg
	if usedCPU > totalCPU {
		usedCPU = totalCPU
	}

	// System RAM
	totalRAM, usedRAM = getSystemRAM()

	return
}

func getLoadAvg() float64 {
	// Linux: /proc/loadavg
	data, err := os.ReadFile("/proc/loadavg")
	if err == nil {
		fields := strings.Fields(string(data))
		if len(fields) > 0 {
			val, _ := strconv.ParseFloat(fields[0], 64)
			return val
		}
	}
	// macOS: sysctl
	out, err := exec.Command("sysctl", "-n", "vm.loadavg").Output()
	if err == nil {
		// output: "{ 1.23 1.45 1.67 }"
		s := strings.Trim(string(out), "{ }\n")
		fields := strings.Fields(s)
		if len(fields) > 0 {
			val, _ := strconv.ParseFloat(fields[0], 64)
			return val
		}
	}
	return float64(runtime.NumCPU()) * 0.1
}

func getSystemRAM() (total, used int64) {
	// macOS: sysctl hw.memsize (total in bytes)
	out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
	if err == nil {
		val, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
		total = val / 1024 / 1024 // bytes to MB
	}

	// macOS: vm_stat for used memory
	out, err = exec.Command("vm_stat").Output()
	if err == nil {
		var pagesActive, pagesWired int64
		for _, line := range strings.Split(string(out), "\n") {
			if strings.Contains(line, "Pages active") {
				pagesActive = parseVmStatValue(line)
			}
			if strings.Contains(line, "Pages wired") {
				pagesWired = parseVmStatValue(line)
			}
		}
		// Each page is 16KB on Apple Silicon, 4KB on Intel
		pageSize := int64(16384) // default Apple Silicon
		if runtime.GOARCH == "amd64" {
			pageSize = 4096
		}
		used = (pagesActive + pagesWired) * pageSize / 1024 / 1024 // MB
	}

	// Linux fallback: /proc/meminfo
	if total == 0 {
		data, err := os.ReadFile("/proc/meminfo")
		if err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if strings.HasPrefix(line, "MemTotal:") {
					total = parseMemInfoKB(line) / 1024
				}
				if strings.HasPrefix(line, "MemAvailable:") {
					available := parseMemInfoKB(line) / 1024
					used = total - available
				}
			}
		}
	}

	if total == 0 {
		total = 1024 // fallback 1GB
	}
	return
}

func parseVmStatValue(line string) int64 {
	parts := strings.Split(line, ":")
	if len(parts) < 2 {
		return 0
	}
	s := strings.TrimSpace(parts[1])
	s = strings.TrimSuffix(s, ".")
	val, _ := strconv.ParseInt(s, 10, 64)
	return val
}

func parseMemInfoKB(line string) int64 {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	val, _ := strconv.ParseInt(fields[1], 10, 64)
	return val
}
