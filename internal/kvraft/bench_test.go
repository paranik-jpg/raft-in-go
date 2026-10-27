package kvraft

import (
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"raft-go/internal/raft"
)

// BenchmarkResults records empirical performance metrics.
type BenchmarkResults struct {
	ClusterSize   int
	TotalWrites   int
	Duration      time.Duration
	Throughput    float64       // writes / sec
	MedianLatency time.Duration // p50
	P95Latency    time.Duration // p95
	P99Latency    time.Duration // p99
}

func (r BenchmarkResults) String() string {
	return fmt.Sprintf("Cluster Size: %d | Total Writes: %d in %v | Throughput: %.2f writes/sec | p50: %v | p95: %v | p99: %v",
		r.ClusterSize, r.TotalWrites, r.Duration, r.Throughput, r.MedianLatency, r.P95Latency, r.P99Latency)
}

// runWriteBenchmark executes concurrent client Put/Append operations across a cluster of given size.
func runWriteBenchmark(clusterSize int, numClients int, writesPerClient int) BenchmarkResults {
	servers, _, cleanup := setupCluster(&testing.T{}, clusterSize, -1)
	defer cleanup()

	totalWrites := numClients * writesPerClient
	latencies := make([]time.Duration, totalWrites)
	var mu sync.Mutex
	idx := 0

	var wg sync.WaitGroup
	startAll := time.Now()

	for c := range numClients {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			ck := NewClerk(servers)
			for w := range writesPerClient {
				key := fmt.Sprintf("bench_key_%d", rand.Intn(100))
				val := fmt.Sprintf("bench_val_%d_%d", clientID, w)

				start := time.Now()
				ck.Put(key, val)
				elapsed := time.Since(start)

				mu.Lock()
				latencies[idx] = elapsed
				idx++
				mu.Unlock()
			}
		}(c)
	}

	wg.Wait()
	totalDuration := time.Since(startAll)

	// Calculate statistics
	slices.Sort(latencies)

	median := latencies[totalWrites*50/100]
	p95 := latencies[totalWrites*95/100]
	p99 := latencies[totalWrites*99/100]
	throughput := float64(totalWrites) / totalDuration.Seconds()

	return BenchmarkResults{
		ClusterSize:   clusterSize,
		TotalWrites:   totalWrites,
		Duration:      totalDuration,
		Throughput:    throughput,
		MedianLatency: median,
		P95Latency:    p95,
		P99Latency:    p99,
	}
}

// MeasureFailoverConvergence quantifies failover speed:
// Measures the exact duration (in milliseconds) from issuing a SIGKILL (Kill/Stop)
// to the active leader until the first successful client write is committed
// by the newly elected leader.
func MeasureFailoverConvergence(clusterSize int) (time.Duration, error) {
	servers, rafts, cleanup := setupCluster(&testing.T{}, clusterSize, -1)
	defer cleanup()

	ck := NewClerk(servers)

	// Step 1: Execute an initial write to ensure stable leadership and active communication
	ck.Put("health_check", "ok")

	// Step 2: Identify the current active leader
	leaderID := -1
	for i := range clusterSize {
		if rafts[i].GetRole() == raft.RoleLeader {
			leaderID = i
			break
		}
	}
	if leaderID == -1 {
		return 0, fmt.Errorf("no active leader found in cluster")
	}

	// Step 3: Issue SIGKILL to the active leader and start timer simultaneously
	killTime := time.Now()
	servers[leaderID].Kill()
	rafts[leaderID].Stop()

	// Step 4: Issue a client write through the Clerk.
	// The Clerk will encounter ErrWrongLeader / timeout against the dead leader,
	// iterate to other peers, wait for the follower timeout (150ms-300ms),
	// elect a new leader, and commit the write.
	ck.Put("failover_probe_key", "committed_by_new_leader")
	failoverDuration := time.Since(killTime)

	// Step 5: Verify the newly elected leader successfully committed and applied the probe
	val := ck.Get("failover_probe_key")
	if val != "committed_by_new_leader" {
		return 0, fmt.Errorf("failover verification failed: expected value not found")
	}

	return failoverDuration, nil
}

// TestEmpiricalEvaluation collects concrete metrics for evaluations across 3-node and 5-node clusters.
func TestEmpiricalEvaluation(t *testing.T) {
	fmt.Println("\n================================================================================")
	fmt.Println("🚀 RUNNING EMPIRICAL BENCHMARK EVALUATIONS")
	fmt.Println("================================================================================")

	// 1. 3-node local cluster benchmark (Write Throughput & Latency)
	res3 := runWriteBenchmark(3, 5, 40) // 200 writes
	fmt.Printf("[3-Node Cluster] => %s\n", res3)

	// 2. 5-node local cluster benchmark (Write Throughput & Latency)
	res5 := runWriteBenchmark(5, 5, 40) // 200 writes
	fmt.Printf("[5-Node Cluster] => %s\n", res5)

	fmt.Println("\n--------------------------------------------------------------------------------")
	fmt.Println("⚡ MEASURING ELECTION & FAILOVER CONVERGENCE TIME (SIGKILL to Leader)")
	fmt.Println("--------------------------------------------------------------------------------")

	// Failover in 3-Node Cluster
	dur3, err := MeasureFailoverConvergence(3)
	if err != nil {
		t.Fatalf("failover test failed on 3 nodes: %v", err)
	}
	fmt.Printf("[3-Node Cluster Failover] => %v (%.2f ms)\n", dur3, float64(dur3.Microseconds())/1000.0)

	// Failover in 5-Node Cluster
	dur5, err := MeasureFailoverConvergence(5)
	if err != nil {
		t.Fatalf("failover test failed on 5 nodes: %v", err)
	}
	fmt.Printf("[5-Node Cluster Failover] => %v (%.2f ms)\n", dur5, float64(dur5.Microseconds())/1000.0)

	fmt.Println("\n--------------------------------------------------------------------------------")
	fmt.Println("📊 MEASURING LINEARIZABLE READ OVERHEAD (Naive Local vs. ReadIndex)")
	fmt.Println("--------------------------------------------------------------------------------")
	benchReadOverhead(3, 500)
	fmt.Println("================================================================================")
}

// benchReadOverhead measures throughput and latency differences between naive local reads
// vs. verified linearizable reads (ReadIndex with heartbeat quorum roundtrips).
func benchReadOverhead(clusterSize int, totalReads int) {
	servers, _, cleanup := setupCluster(&testing.T{}, clusterSize, -1)
	defer cleanup()

	ck := NewClerk(servers)
	// Seed initial data
	ck.Put("bench_read_key", "bench_read_val")

	// 1. Benchmark Naive Local Reads (No consensus, memory-only)
	startLocal := time.Now()
	for range totalReads {
		_ = ck.GetLocal("bench_read_key")
	}
	durLocal := time.Since(startLocal)
	tpLocal := float64(totalReads) / durLocal.Seconds()
	avgLocal := durLocal / time.Duration(totalReads)

	// 2. Benchmark Verified Linearizable Reads (ReadIndex with heartbeat quorum roundtrips)
	startLin := time.Now()
	for range totalReads {
		_ = ck.GetLinearizable("bench_read_key")
	}
	durLin := time.Since(startLin)
	tpLin := float64(totalReads) / durLin.Seconds()
	avgLin := durLin / time.Duration(totalReads)

	fmt.Printf("Naive Local Reads        => %d reads in %v | Throughput: %10.2f reads/sec | Avg Latency: %v\n",
		totalReads, durLocal, tpLocal, avgLocal)
	fmt.Printf("Linearizable (ReadIndex) => %d reads in %v | Throughput: %10.2f reads/sec | Avg Latency: %v\n",
		totalReads, durLin, tpLin, avgLin)
	fmt.Printf("Consensus Safety Tax     => Local is %.2fx faster than quorum-verified linearizable reads\n",
		tpLocal/tpLin)
}

