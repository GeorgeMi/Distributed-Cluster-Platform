package agent

import (
	"os"
	"runtime"
	"strconv"
	"strings"
)

func GetMetrics() (totalCPU, usedCPU float64, totalRAM, usedRAM int64) {
	totalCPU = float64(runtime.NumCPU())

	// Approximate CPU usage from load average (unix only)
	loadAvg := getLoadAvg()
	usedCPU = loadAvg
	if usedCPU > totalCPU {
		usedCPU = totalCPU
	}

	// RAM from runtime
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	totalRAM = int64(mem.Sys / 1024 / 1024)       // MB
	usedRAM = int64(mem.Alloc / 1024 / 1024)       // MB

	return
}

func getLoadAvg() float64 {
	data, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		// macOS: try sysctl
		return float64(runtime.NumCPU()) * 0.1 // fallback: 10% usage
	}
	fields := strings.Fields(string(data))
	if len(fields) == 0 {
		return 0
	}
	val, _ := strconv.ParseFloat(fields[0], 64)
	return val
}
