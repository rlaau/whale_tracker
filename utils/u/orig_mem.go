package main

// import (
// 	"fmt"
// 	"math/rand"
// 	"runtime"
// 	"time"
// )

// // Element represents an element in our union-find structure
// // Using [20]byte as specified in the requirement
// type Element [20]byte

// // Node represents a node in the union-find structure
// type Node struct {
// 	Parent *Node
// 	Rank   int
// 	Value  Element
// 	Size   int
// }

// // UnionFind represents the union-find data structure
// type UnionFind struct {
// 	Nodes map[Element]*Node
// }

// // NewUnionFind creates a new union-find data structure
// func NewUnionFind() *UnionFind {
// 	return &UnionFind{
// 		Nodes: make(map[Element]*Node),
// 	}
// }

// // MakeSet creates a new set with the given element
// func (uf *UnionFind) MakeSet(element Element) *Node {
// 	if node, exists := uf.Nodes[element]; exists {
// 		return node
// 	}

// 	node := &Node{
// 		Value: element,
// 		Size:  1,
// 	}
// 	node.Parent = node // self-parent initially
// 	uf.Nodes[element] = node
// 	return node
// }

// // Find returns the representative (root) of the set containing element
// func (uf *UnionFind) Find(element Element) *Node {
// 	if node, exists := uf.Nodes[element]; exists {
// 		return uf.FindRoot(node)
// 	}
// 	return nil
// }

// // FindRoot returns the representative (root) of the set containing node
// // With path compression for efficiency
// func (uf *UnionFind) FindRoot(node *Node) *Node {
// 	if node.Parent != node {
// 		node.Parent = uf.FindRoot(node.Parent) // Path compression
// 	}
// 	return node.Parent
// }

// // Union merges the sets containing elements a and b
// // Returns true if a merge occurred, false if they were already in the same set
// func (uf *UnionFind) Union(a, b Element) bool {
// 	rootA := uf.Find(a)
// 	rootB := uf.Find(b)

// 	if rootA == nil || rootB == nil {
// 		return false
// 	}

// 	if rootA == rootB {
// 		return false // Already in the same set
// 	}

// 	// Union by rank
// 	if rootA.Rank < rootB.Rank {
// 		rootA.Parent = rootB
// 		rootB.Size += rootA.Size
// 	} else if rootA.Rank > rootB.Rank {
// 		rootB.Parent = rootA
// 		rootA.Size += rootB.Size
// 	} else {
// 		rootB.Parent = rootA
// 		rootA.Rank++
// 		rootA.Size += rootB.Size
// 	}

// 	return true
// }

// // GenerateRandomElement creates a random [20]byte element
// func GenerateRandomElement() Element {
// 	var element Element
// 	for i := range element {
// 		element[i] = byte(rand.Intn(256))
// 	}
// 	return element
// }

// // MemoryStats captures current memory statistics
// type MemoryStats struct {
// 	Alloc      uint64
// 	TotalAlloc uint64
// 	Sys        uint64
// 	NumGC      uint32
// }

// // GetMemoryStats returns current memory statistics
// func GetMemoryStats() MemoryStats {
// 	var stats runtime.MemStats
// 	runtime.ReadMemStats(&stats)
// 	return MemoryStats{
// 		Alloc:      stats.Alloc,
// 		TotalAlloc: stats.TotalAlloc,
// 		Sys:        stats.Sys,
// 		NumGC:      stats.NumGC,
// 	}
// }

// // PrintMemoryUsage prints memory usage statistics
// func PrintMemoryUsage(label string, start, current MemoryStats) {
// 	fmt.Printf("=== %s ===\n", label)
// 	fmt.Printf("Current allocation: %v MB\n", float64(current.Alloc)/1024/1024)
// 	fmt.Printf("System memory: %v MB\n", float64(current.Sys)/1024/1024)
// 	fmt.Printf("Allocation change: %v MB\n", float64(current.Alloc-start.Alloc)/1024/1024)
// 	fmt.Printf("Total allocations: %v MB\n", float64(current.TotalAlloc)/1024/1024)
// 	fmt.Printf("GC runs: %v\n", current.NumGC)
// 	fmt.Println()
// }

// func main() {
// 	// Seed the random number generator
// 	rand.Seed(time.Now().UnixNano())

// 	// Number of elements and sets
// 	const numElements = 20_000_000

// 	// Track memory usage
// 	startStats := GetMemoryStats()
// 	fmt.Println("Starting memory monitoring...")
// 	PrintMemoryUsage("Initial", startStats, startStats)

// 	// Create sets
// 	uf := NewUnionFind()

// 	// Generate our elements first
// 	elements := make([]Element, numElements)

// 	fmt.Println("Generating elements...")
// 	preGenStats := GetMemoryStats()
// 	for i := 0; i < numElements; i++ {
// 		elements[i] = GenerateRandomElement()
// 	}
// 	genStats := GetMemoryStats()
// 	PrintMemoryUsage("After generating elements", preGenStats, genStats)

// 	// Create the sets
// 	fmt.Println("Creating initial sets...")
// 	preMakeSetStats := GetMemoryStats()
// 	for i := 0; i < numElements; i++ {
// 		uf.MakeSet(elements[i])

// 		// Print progress periodically
// 		if (i+1)%1_000_000 == 0 {
// 			fmt.Printf("Created %d sets...\n", i+1)
// 			currentStats := GetMemoryStats()
// 			PrintMemoryUsage(fmt.Sprintf("After creating %d sets", i+1), preMakeSetStats, currentStats)
// 		}
// 	}

// 	postMakeSetStats := GetMemoryStats()
// 	PrintMemoryUsage("After creating all sets", preMakeSetStats, postMakeSetStats)

// 	// Now perform unions to create sets of sizes 1-4
// 	// 80% of sets will remain size 1, 20% will be merged into sets of size 2-4

// 	fmt.Println("Performing unions to create larger sets...")
// 	preUnionStats := GetMemoryStats()

// 	// Calculate how many elements to merge (20% of total)
// 	elemsToMerge := int(float64(numElements) * 0.2)

// 	// Track how many have been merged so far
// 	mergedCount := 0

// 	// Track sets by target size
// 	targetSets := make(map[int][]Element)

// 	// Initialize with sizes 2, 3, 4
// 	for size := 2; size <= 4; size++ {
// 		targetSets[size] = make([]Element, 0)
// 	}

// 	// Assign elements to target set sizes (determine which elements go into which size sets)
// 	for i := 0; i < elemsToMerge; i++ {
// 		// Select a random size between 2-4, with equal probability
// 		targetSize := rand.Intn(3) + 2
// 		targetSets[targetSize] = append(targetSets[targetSize], elements[i])
// 	}

// 	// Create the sets of different sizes
// 	for size := 2; size <= 4; size++ {
// 		targetElems := targetSets[size]

// 		fmt.Printf("Creating sets of size %d...\n", size)

// 		// Group elements for sets of this size
// 		numSets := len(targetElems) / size
// 		for i := 0; i < numSets; i++ {
// 			// Base element becomes the "root" of this set
// 			baseElem := targetElems[i*size]

// 			// Merge other elements into this set
// 			for j := 1; j < size; j++ {
// 				if i*size+j < len(targetElems) {
// 					elem := targetElems[i*size+j]
// 					uf.Union(baseElem, elem)
// 					mergedCount++

// 					// Print memory stats periodically
// 					if mergedCount%1_000_000 == 0 {
// 						fmt.Printf("Merged %d elements...\n", mergedCount)
// 						currentStats := GetMemoryStats()
// 						PrintMemoryUsage(fmt.Sprintf("After %d unions", mergedCount), preUnionStats, currentStats)
// 					}
// 				}
// 			}
// 		}
// 	}

// 	postUnionStats := GetMemoryStats()
// 	PrintMemoryUsage("After all unions", preUnionStats, postUnionStats)

// 	// Verify the distribution of set sizes
// 	sizeCounts := make(map[int]int)

// 	fmt.Println("Verifying set size distribution...")
// 	for _, elem := range elements {
// 		root := uf.Find(elem)
// 		if root != nil {
// 			sizeCounts[root.Size]++
// 		}
// 	}

// 	// Print the counts of sets of each size
// 	fmt.Println("Set size distribution:")
// 	for size := 1; size <= 4; size++ {
// 		fmt.Printf("Sets of size %d: %d\n", size, sizeCounts[size])
// 	}

// 	// Final memory stats
// 	finalStats := GetMemoryStats()
// 	PrintMemoryUsage("Final", startStats, finalStats)
// }
