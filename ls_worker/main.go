package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"syscall"
)

type InBudget struct {
	MinRuntimeSec int   `json:"min_runtime_sec"`
	TimeLimitSec  int   `json:"time_limit_sec"`
	MaxSteps      int64 `json:"max_steps"`
	MaxNodes      int64 `json:"max_nodes"`
}

type InOutput struct {
	ReturnOneSolution bool `json:"return_one_solution"`
	ReturnSquares     bool `json:"return_squares"`
	MaxSolutions      int  `json:"max_solutions"`
}

type InRequest struct {
	TaskID  string          `json:"task_id"`
	Problem string          `json:"problem"`
	Budget  InBudget        `json:"budget"`
	Seed    int64           `json:"seed"`
	Output  InOutput        `json:"output"`
	Payload json.RawMessage `json:"payload"`
}

type OutError struct {
	Code    string                 `json:"code"`
	Message string                 `json:"message"`
	Details map[string]interface{} `json:"details,omitempty"`
}

type OutMetrics struct {
	StartedAtUnix  int64  `json:"started_at_unix"`
	FinishedAtUnix int64  `json:"finished_at_unix"`
	WallMS         int64  `json:"wall_ms"`
	CPUUserMS      int64  `json:"cpu_user_ms"`
	CPUSysMS       int64  `json:"cpu_sys_ms"`
	MaxRSSKB       int64  `json:"max_rss_kb"`
	Hostname       string `json:"hostname"`
	PID            int    `json:"pid"`
	GOOS           string `json:"goos"`
	GOARCH         string `json:"goarch"`
	CoresSeen      int    `json:"cores_seen"`
}

type OutResponse struct {
	Ok      bool        `json:"ok"`
	Problem string      `json:"problem"`
	TaskID  string      `json:"task_id,omitempty"`
	Status  string      `json:"status"` // done | no_solution | timeout | invalid_input | error
	Result  interface{} `json:"result,omitempty"`
	Metrics OutMetrics  `json:"metrics"`
	Debug   interface{} `json:"debug,omitempty"`
	Error   *OutError   `json:"error,omitempty"`
}

// ---------------------------
// Payloads
// ---------------------------

type PayloadComplete struct {
	N            int      `json:"n"`
	PrefixFormat string   `json:"prefix_format"`
	Prefix       [][]*int `json:"prefix"`
	Constraints  struct {
		Latin           bool `json:"latin"`
		SymmetryBreaking struct {
			FixFirstRow bool `json:"fix_first_row"`
		} `json:"symmetry_breaking"`
	} `json:"constraints"`
}

type PayloadMOLS struct {
	N      int    `json:"n"`
	K      int    `json:"k"`
	Method string `json:"method"`
}

type ResultComplete struct {
	N            int     `json:"n"`
	SolutionFound bool    `json:"solution_found"`
	Square       [][]int `json:"square,omitempty"`
	VerifiedLatin bool   `json:"verified_latin"`
}

type ResultMOLS struct {
	N          int         `json:"n"`
	K          int         `json:"k"`
	Found      bool        `json:"found"`
	Conflicts  int         `json:"conflicts"`
	UniquePairs int        `json:"unique_pairs"`
	L          [][][]int   `json:"L,omitempty"`
	BestHash   []string    `json:"best_hash,omitempty"`
}

type DebugInfo struct {
	Attempts  int     `json:"attempts,omitempty"`
	BestScore int     `json:"best_score,omitempty"`
	Notes     string  `json:"notes,omitempty"`
	Steps     int64   `json:"steps,omitempty"`
	Nodes     int64   `json:"nodes,omitempty"`
}

// ---------------------------
// Main
// ---------------------------

func main() {
	inPath := flag.String("in", "in.json", "input json path")
	outPath := flag.String("out", "out.json", "output json path")
	flag.Parse()

	startWall := time.Now()
	startUnix := startWall.Unix()

	host, _ := os.Hostname()

	req, err := readIn(*inPath)
	if err != nil {
		writeOut(*outPath, OutResponse{
			Ok:      false,
			Problem: "",
			Status:  "invalid_input",
			Metrics: finishMetrics(startUnix, startWall, host),
			Error: &OutError{
				Code:    "BAD_JSON",
				Message: err.Error(),
			},
		})
		os.Exit(2)
	}

	// Defaults
	if req.Budget.MinRuntimeSec <= 0 {
		req.Budget.MinRuntimeSec = 5
	}
	if req.Budget.TimeLimitSec <= 0 {
		req.Budget.TimeLimitSec = 60
	}
	if req.Budget.TimeLimitSec > 1800 {
		req.Budget.TimeLimitSec = 1800
	}
	if req.Output.MaxSolutions <= 0 {
		req.Output.MaxSolutions = 1
	}

	deadline := startWall.Add(time.Duration(req.Budget.TimeLimitSec) * time.Second)
	rng := rand.New(rand.NewSource(req.Seed))

	var resp OutResponse
	resp.Problem = req.Problem
	resp.TaskID = req.TaskID

	switch req.Problem {
	case "complete_latin_square_from_prefix":
		resp = handleComplete(req, rng, deadline, startUnix, startWall, host)
	case "search_mols":
		resp = handleMOLS(req, rng, deadline, startUnix, startWall, host)
	default:
		resp = OutResponse{
			Ok:      false,
			Problem: req.Problem,
			TaskID:  req.TaskID,
			Status:  "invalid_input",
			Metrics: finishMetrics(startUnix, startWall, host),
			Error: &OutError{
				Code:    "UNKNOWN_PROBLEM",
				Message: fmt.Sprintf("unknown problem=%q", req.Problem),
			},
		}
	}

	// min_runtime: если закончили раньше — дожигаем
	minEnd := startWall.Add(time.Duration(req.Budget.MinRuntimeSec) * time.Second)
	if time.Now().Before(minEnd) {
		time.Sleep(time.Until(minEnd))
	}

	// перезапишем метрики после min_runtime sleep
	resp.Metrics = finishMetrics(startUnix, startWall, host)
	writeOut(*outPath, resp)

	if resp.Ok {
		os.Exit(0)
	}
	os.Exit(1)
}

func readIn(path string) (InRequest, error) {
	var req InRequest
	b, err := os.ReadFile(path)
	if err != nil {
		return req, fmt.Errorf("read %s: %w", path, err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields() // чтобы ловить опечатки в ключах
	if err := dec.Decode(&req); err != nil {
		return req, fmt.Errorf("decode json: %w", err)
	}
	req.Problem = strings.TrimSpace(req.Problem)
	return req, nil
}

func writeOut(path string, resp OutResponse) {
	b, _ := json.MarshalIndent(resp, "", "  ")
	_ = os.WriteFile(path, b, 0644)
}

func finishMetrics(startUnix int64, startWall time.Time, host string) OutMetrics {
	endWall := time.Now()
	endUnix := endWall.Unix()
	wallMS := endWall.Sub(startWall).Milliseconds()

	ru := &syscall.Rusage{}
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, ru)

	cpuUserMS := timevalToMS(ru.Utime)
	cpuSysMS := timevalToMS(ru.Stime)
	// Linux: Maxrss в KB (обычно). Для курсовой норм как есть.
	maxRSSKB := int64(ru.Maxrss)

	return OutMetrics{
		StartedAtUnix:  startUnix,
		FinishedAtUnix: endUnix,
		WallMS:         wallMS,
		CPUUserMS:      cpuUserMS,
		CPUSysMS:       cpuSysMS,
		MaxRSSKB:       maxRSSKB,
		Hostname:       host,
		PID:            os.Getpid(),
		GOOS:           runtime.GOOS,
		GOARCH:         runtime.GOARCH,
		CoresSeen:      runtime.NumCPU(),
	}
}

func timevalToMS(tv syscall.Timeval) int64 {
	// tv.Sec seconds + tv.Usec microseconds
	return tv.Sec*1000 + int64(tv.Usec)/1000
}

// ---------------------------
// COMPLETE: Latin square completion
// ---------------------------

func handleComplete(req InRequest, rng *rand.Rand, deadline time.Time, startUnix int64, startWall time.Time, host string) OutResponse {
	var p PayloadComplete
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return OutResponse{
			Ok:      false,
			Problem: req.Problem,
			TaskID:  req.TaskID,
			Status:  "invalid_input",
			Metrics: finishMetrics(startUnix, startWall, host),
			Error: &OutError{
				Code:    "BAD_PAYLOAD",
				Message: err.Error(),
			},
		}
	}

	// validate basic
	if p.N <= 0 {
		return invalid("BAD_N", "n must be > 0", req, startUnix, startWall, host)
	}
	if len(p.Prefix) != p.N {
		return invalid("BAD_PREFIX_SHAPE", "prefix must be n x n", req, startUnix, startWall, host)
	}
	for i := range p.Prefix {
		if len(p.Prefix[i]) != p.N {
			return invalid("BAD_PREFIX_SHAPE", "prefix must be n x n", req, startUnix, startWall, host)
		}
	}

	// build board
	n := p.N
	board := make([][]int, n)
	fixed := make([][]bool, n)
	for i := 0; i < n; i++ {
		board[i] = make([]int, n)
		fixed[i] = make([]bool, n)
		for j := 0; j < n; j++ {
			if p.Prefix[i][j] == nil {
				board[i][j] = -1
			} else {
				v := *p.Prefix[i][j]
				if v < 0 || v >= n {
					return invalid("BAD_VALUE", fmt.Sprintf("value out of range at (%d,%d)", i, j), req, startUnix, startWall, host)
				}
				board[i][j] = v
				fixed[i][j] = true
			}
		}
	}

	if p.Constraints.SymmetryBreaking.FixFirstRow {
		// first row must be full permutation 0..n-1
		seen := make([]bool, n)
		for j := 0; j < n; j++ {
			if board[0][j] < 0 {
				return invalid("FIX_FIRST_ROW", "first row must be fully specified when fix_first_row=true", req, startUnix, startWall, host)
			}
			v := board[0][j]
			if seen[v] {
				return invalid("FIX_FIRST_ROW", "first row must be a permutation (no duplicates)", req, startUnix, startWall, host)
			}
			seen[v] = true
		}
	}

	// check prefix consistency (no duplicates in row/col)
	if err := validatePartialLatin(board); err != nil {
		return OutResponse{
			Ok:      false,
			Problem: req.Problem,
			TaskID:  req.TaskID,
			Status:  "invalid_input",
			Metrics: finishMetrics(startUnix, startWall, host),
			Error: &OutError{
				Code:    "INVALID_PREFIX",
				Message: err.Error(),
			},
		}
	}

	maxNodes := req.Budget.MaxNodes
	if maxNodes <= 0 {
		maxNodes = 3_000_000
	}

	solver := newLSSolver(board, fixed)
	solver.rng = rng
	solver.deadline = deadline
	solver.maxNodes = maxNodes

	ok, status, nodes := solver.solve()
	res := ResultComplete{
		N:            n,
		SolutionFound: ok,
		Square:       nil,
		VerifiedLatin: false,
	}
	if ok {
		res.Square = solver.board
		res.VerifiedLatin = isLatinSquare(solver.board)
	}

	debug := DebugInfo{Nodes: nodes}

	return OutResponse{
		Ok:      ok || status == "timeout", // timeout тоже “валидный” результат попытки
		Problem: req.Problem,
		TaskID:  req.TaskID,
		Status:  status,
		Result:  res,
		Debug:   debug,
		Metrics: finishMetrics(startUnix, startWall, host),
		Error:   nil,
	}
}

func invalid(code, msg string, req InRequest, startUnix int64, startWall time.Time, host string) OutResponse {
	return OutResponse{
		Ok:      false,
		Problem: req.Problem,
		TaskID:  req.TaskID,
		Status:  "invalid_input",
		Metrics: finishMetrics(startUnix, startWall, host),
		Error: &OutError{
			Code:    code,
			Message: msg,
		},
	}
}

func validatePartialLatin(board [][]int) error {
	n := len(board)
	// rows
	for i := 0; i < n; i++ {
		seen := make([]bool, n)
		for j := 0; j < n; j++ {
			v := board[i][j]
			if v < 0 {
				continue
			}
			if seen[v] {
				return fmt.Errorf("duplicate value %d in row %d", v, i)
			}
			seen[v] = true
		}
	}
	// cols
	for j := 0; j < n; j++ {
		seen := make([]bool, n)
		for i := 0; i < n; i++ {
			v := board[i][j]
			if v < 0 {
				continue
			}
			if seen[v] {
				return fmt.Errorf("duplicate value %d in col %d", v, j)
			}
			seen[v] = true
		}
	}
	return nil
}

func isLatinSquare(board [][]int) bool {
	n := len(board)
	for i := 0; i < n; i++ {
		seen := make([]bool, n)
		for j := 0; j < n; j++ {
			v := board[i][j]
			if v < 0 || v >= n || seen[v] {
				return false
			}
			seen[v] = true
		}
	}
	for j := 0; j < n; j++ {
		seen := make([]bool, n)
		for i := 0; i < n; i++ {
			v := board[i][j]
			if v < 0 || v >= n || seen[v] {
				return false
			}
			seen[v] = true
		}
	}
	return true
}

type lsSolver struct {
	board    [][]int
	fixed    [][]bool
	n        int
	rowMask  []uint64
	colMask  []uint64
	deadline time.Time
	maxNodes int64
	nodes    int64
	rng      *rand.Rand
}

func newLSSolver(board [][]int, fixed [][]bool) *lsSolver {
	n := len(board)
	s := &lsSolver{
		n:       n,
		board:   deepCopy(board),
		fixed:   fixed,
		rowMask: make([]uint64, n),
		colMask: make([]uint64, n),
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			v := s.board[i][j]
			if v >= 0 {
				s.rowMask[i] |= (1 << uint(v))
				s.colMask[j] |= (1 << uint(v))
			}
		}
	}
	return s
}

func deepCopy(a [][]int) [][]int {
	n := len(a)
	out := make([][]int, n)
	for i := 0; i < n; i++ {
		out[i] = make([]int, len(a[i]))
		copy(out[i], a[i])
	}
	return out
}

func (s *lsSolver) solve() (bool, string, int64) {
	ok := s.dfs()
	if ok {
		return true, "done", s.nodes
	}
	// если остановились по времени/лимиту
	if time.Now().After(s.deadline) || (s.maxNodes > 0 && s.nodes >= s.maxNodes) {
		return false, "timeout", s.nodes
	}
	return false, "no_solution", s.nodes
}

func (s *lsSolver) dfs() bool {
	if time.Now().After(s.deadline) {
		return false
	}
	if s.maxNodes > 0 && s.nodes >= s.maxNodes {
		return false
	}

	// find next cell with MRV (min candidates)
	iBest, jBest := -1, -1
	var candBest []int
	bestLen := math.MaxInt32

	for i := 0; i < s.n; i++ {
		for j := 0; j < s.n; j++ {
			if s.board[i][j] != -1 {
				continue
			}
			cands := s.candidates(i, j)
			if len(cands) == 0 {
				return false
			}
			if len(cands) < bestLen {
				bestLen = len(cands)
				iBest, jBest = i, j
				candBest = cands
				if bestLen == 1 {
					break
				}
			}
		}
	}

	if iBest == -1 {
		// filled
		return true
	}

	// randomize candidate order using seed
	s.shuffleInts(candBest)

	for _, v := range candBest {
		s.nodes++
		s.place(iBest, jBest, v)
		if s.dfs() {
			return true
		}
		s.unplace(iBest, jBest, v)
	}
	return false
}

func (s *lsSolver) candidates(i, j int) []int {
	used := s.rowMask[i] | s.colMask[j]
	cands := make([]int, 0, s.n)
	for v := 0; v < s.n; v++ {
		if (used & (1 << uint(v))) == 0 {
			cands = append(cands, v)
		}
	}
	return cands
}

func (s *lsSolver) place(i, j, v int) {
	s.board[i][j] = v
	s.rowMask[i] |= (1 << uint(v))
	s.colMask[j] |= (1 << uint(v))
}

func (s *lsSolver) unplace(i, j, v int) {
	s.board[i][j] = -1
	s.rowMask[i] &^= (1 << uint(v))
	s.colMask[j] &^= (1 << uint(v))
}

func (s *lsSolver) shuffleInts(a []int) {
	if s.rng == nil {
		return
	}
	for i := len(a) - 1; i > 0; i-- {
		j := s.rng.Intn(i + 1)
		a[i], a[j] = a[j], a[i]
	}
}

// ---------------------------
// MOLS: simple stochastic “best conflicts” search
// ---------------------------

func handleMOLS(req InRequest, rng *rand.Rand, deadline time.Time, startUnix int64, startWall time.Time, host string) OutResponse {
	var p PayloadMOLS
	if err := json.Unmarshal(req.Payload, &p); err != nil {
		return invalid("BAD_PAYLOAD", err.Error(), req, startUnix, startWall, host)
	}
	if p.N <= 0 {
		return invalid("BAD_N", "n must be > 0", req, startUnix, startWall, host)
	}
	if p.K < 2 || p.K > p.N-1 {
		return invalid("BAD_K", "k must be in [2, n-1]", req, startUnix, startWall, host)
	}
	// быстрый теоретический стоп для пары
	if p.K == 2 && (p.N == 2 || p.N == 6) {
		res := ResultMOLS{N: p.N, K: p.K, Found: false, Conflicts: p.N * p.N, UniquePairs: 0}
		return OutResponse{
			Ok:      true,
			Problem: req.Problem,
			TaskID:  req.TaskID,
			Status:  "no_solution",
			Result:  res,
			Debug:   DebugInfo{Notes: "No orthogonal pair exists for n=2 or n=6 (k=2)."},
			Metrics: finishMetrics(startUnix, startWall, host),
		}
	}

	n := p.N
	k := p.K
	if k != 2 {
		// пока честно поддержим только k=2 (иначе усложнение резко)
		return OutResponse{
			Ok:      false,
			Problem: req.Problem,
			TaskID:  req.TaskID,
			Status:  "error",
			Metrics: finishMetrics(startUnix, startWall, host),
			Error: &OutError{
				Code:    "NOT_IMPLEMENTED",
				Message: "currently supports only k=2",
			},
		}
	}

	maxSteps := req.Budget.MaxSteps
	if maxSteps <= 0 {
		maxSteps = 2_000_000
	}

	// старт: L0 = cyclic latin
	L0 := makeCyclicLatin(n, 1)
	// L1 стартуем как тоже cyclic, но потом мутируем перестановками
	L1 := makeCyclicLatin(n, 1)
	// рандомные перестановки (сохраняют латинскость)
	randomPermuteLatin(L0, rng)
	randomPermuteLatin(L1, rng)

	bestConf, bestUnique := orthConflicts(L0, L1)
	bestL1 := deepCopy(L1)
	steps := int64(0)

	// локальный поиск: пробуем случайные операции, принимаем если лучше
	for steps < maxSteps && time.Now().Before(deadline) {
		steps++

		// копия текущего L1
		cand := deepCopy(L1)

		// случайная операция
		switch rng.Intn(3) {
		case 0:
			// swap two rows
			r1 := rng.Intn(n)
			r2 := rng.Intn(n)
			cand[r1], cand[r2] = cand[r2], cand[r1]
		case 1:
			// swap two cols
			c1 := rng.Intn(n)
			c2 := rng.Intn(n)
			for i := 0; i < n; i++ {
				cand[i][c1], cand[i][c2] = cand[i][c2], cand[i][c1]
			}
		case 2:
			// rename two symbols
			a := rng.Intn(n)
			b := rng.Intn(n)
			if a != b {
				for i := 0; i < n; i++ {
					for j := 0; j < n; j++ {
						if cand[i][j] == a {
							cand[i][j] = b
						} else if cand[i][j] == b {
							cand[i][j] = a
						}
					}
				}
			}
		}

		conf, uniq := orthConflicts(L0, cand)
		// принимаем если лучше, или иногда если равно (чтобы двигаться)
		if conf < bestConf || (conf == bestConf && uniq > bestUnique) {
			L1 = cand
			bestConf, bestUnique = conf, uniq
			bestL1 = deepCopy(cand)
			if bestConf == 0 {
				break
			}
		} else if rng.Float64() < 0.001 {
			L1 = cand // редкий “шаг в сторону”
		}
	}

	found := (bestConf == 0)
	res := ResultMOLS{
		N:           n,
		K:           2,
		Found:       found,
		Conflicts:   bestConf,
		UniquePairs: bestUnique,
	}

	if req.Output.ReturnSquares {
		res.L = [][][]int{L0, bestL1}
	} else {
		res.BestHash = []string{hashSquare(L0), hashSquare(bestL1)}
	}

	status := "done"
	if !found && time.Now().After(deadline) {
		status = "timeout"
	}

	return OutResponse{
		Ok:      true, // даже если не нашли — попытка валидная
		Problem: req.Problem,
		TaskID:  req.TaskID,
		Status:  status,
		Result:  res,
		Debug:   DebugInfo{Steps: steps, BestScore: bestConf},
		Metrics: finishMetrics(startUnix, startWall, host),
	}
}

func makeCyclicLatin(n int, a int) [][]int {
	// L[i][j] = (a*i + j) mod n  (Latin если gcd(a,n)=1; но даже a=1 всегда ок)
	L := make([][]int, n)
	for i := 0; i < n; i++ {
		L[i] = make([]int, n)
		for j := 0; j < n; j++ {
			L[i][j] = (a*i + j) % n
		}
	}
	return L
}

func randomPermuteLatin(L [][]int, rng *rand.Rand) {
	n := len(L)

	// permute rows
	rp := rng.Perm(n)
	tmp := deepCopy(L)
	for i := 0; i < n; i++ {
		L[i] = tmp[rp[i]]
	}

	// permute cols
	cp := rng.Perm(n)
	for i := 0; i < n; i++ {
		row := make([]int, n)
		for j := 0; j < n; j++ {
			row[j] = L[i][cp[j]]
		}
		L[i] = row
	}

	// permute symbols
	sp := rng.Perm(n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			L[i][j] = sp[L[i][j]]
		}
	}
}

func orthConflicts(A, B [][]int) (conflicts int, uniquePairs int) {
	n := len(A)
	seen := make(map[int]bool, n*n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			key := A[i][j]*n + B[i][j]
			seen[key] = true
		}
	}
	uniquePairs = len(seen)
	conflicts = n*n - uniquePairs
	return
}

func hashSquare(L [][]int) string {
	// быстрый “хэш” для отчёта: первые N чисел + checksum
	n := len(L)
	var flat []int
	for i := 0; i < n; i++ {
		flat = append(flat, L[i]...)
	}
	sum := 0
	for _, v := range flat {
		sum = (sum*131 + v + 1) % 1000000007
	}
	// первые 12 элементов для читаемости
	m := 12
	if len(flat) < m {
		m = len(flat)
	}
	head := make([]int, m)
	copy(head, flat[:m])
	sort.Ints(head)
	return fmt.Sprintf("n=%d sum=%d head=%v", n, sum, head)
}
