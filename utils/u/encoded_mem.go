package main

// import (
// 	"fmt"
// 	"math/rand"
// 	"runtime"
// 	"time"
// )

// // UintElement represents an element in our union-find structure
// // Using uint64 as specified in the updated requirement
// type UintElement uint64

// // UintNode represents a node in the union-find structure
// type UintNode struct {
// 	UintParent *UintNode
// 	UintRank   int
// 	UintValue  UintElement
// 	UintSize   int
// }

// // UintUnionFind represents the union-find data structure
// type UintUnionFind struct {
// 	UintNodes map[UintElement]*UintNode
// }

// // NewUintUnionFind creates a new union-find data structure
// func NewUintUnionFind() *UintUnionFind {
// 	return &UintUnionFind{
// 		UintNodes: make(map[UintElement]*UintNode),
// 	}
// }

// // UintMakeSet creates a new set with the given element
// func (uintUF *UintUnionFind) UintMakeSet(uintElement UintElement) *UintNode {
// 	if uintNode, uintExists := uintUF.UintNodes[uintElement]; uintExists {
// 		return uintNode
// 	}

// 	uintNode := &UintNode{
// 		UintValue: uintElement,
// 		UintSize:  1,
// 	}
// 	uintNode.UintParent = uintNode // self-parent initially
// 	uintUF.UintNodes[uintElement] = uintNode
// 	return uintNode
// }

// // UintFind returns the representative (root) of the set containing element
// func (uintUF *UintUnionFind) UintFind(uintElement UintElement) *UintNode {
// 	if uintNode, uintExists := uintUF.UintNodes[uintElement]; uintExists {
// 		return uintUF.UintFindRoot(uintNode)
// 	}
// 	return nil
// }

// // UintFindRoot returns the representative (root) of the set containing node
// // With path compression for efficiency
// func (uintUF *UintUnionFind) UintFindRoot(uintNode *UintNode) *UintNode {
// 	if uintNode.UintParent != uintNode {
// 		uintNode.UintParent = uintUF.UintFindRoot(uintNode.UintParent) // Path compression
// 	}
// 	return uintNode.UintParent
// }

// // UintUnion merges the sets containing elements a and b
// // Returns true if a merge occurred, false if they were already in the same set
// func (uintUF *UintUnionFind) UintUnion(uintA, uintB UintElement) bool {
// 	uintRootA := uintUF.UintFind(uintA)
// 	uintRootB := uintUF.UintFind(uintB)

// 	if uintRootA == nil || uintRootB == nil {
// 		return false
// 	}

// 	if uintRootA == uintRootB {
// 		return false // Already in the same set
// 	}

// 	// Union by rank
// 	if uintRootA.UintRank < uintRootB.UintRank {
// 		uintRootA.UintParent = uintRootB
// 		uintRootB.UintSize += uintRootA.UintSize
// 	} else if uintRootA.UintRank > uintRootB.UintRank {
// 		uintRootB.UintParent = uintRootA
// 		uintRootA.UintSize += uintRootB.UintSize
// 	} else {
// 		uintRootB.UintParent = uintRootA
// 		uintRootA.UintRank++
// 		uintRootA.UintSize += uintRootB.UintSize
// 	}

// 	return true
// }

// // UintGenerateRandomElement creates a random uint64 element
// func UintGenerateRandomElement() UintElement {
// 	return UintElement(rand.Uint64())
// }

// // UintMemoryStats captures current memory statistics
// type UintMemoryStats struct {
// 	UintAlloc      uint64
// 	UintTotalAlloc uint64
// 	UintSys        uint64
// 	UintNumGC      uint32
// }

// // UintGetMemoryStats returns current memory statistics
// func UintGetMemoryStats() UintMemoryStats {
// 	var uintStats runtime.MemStats
// 	runtime.ReadMemStats(&uintStats)
// 	return UintMemoryStats{
// 		UintAlloc:      uintStats.Alloc,
// 		UintTotalAlloc: uintStats.TotalAlloc,
// 		UintSys:        uintStats.Sys,
// 		UintNumGC:      uintStats.NumGC,
// 	}
// }

// // UintPrintMemoryUsage prints memory usage statistics
// func UintPrintMemoryUsage(uintLabel string, uintStart, uintCurrent UintMemoryStats) {
// 	fmt.Printf("=== %s ===\n", uintLabel)
// 	fmt.Printf("Current allocation: %v MB\n", float64(uintCurrent.UintAlloc)/1024/1024)
// 	fmt.Printf("System memory: %v MB\n", float64(uintCurrent.UintSys)/1024/1024)
// 	fmt.Printf("Allocation change: %v MB\n", float64(uintCurrent.UintAlloc-uintStart.UintAlloc)/1024/1024)
// 	fmt.Printf("Total allocations: %v MB\n", float64(uintCurrent.UintTotalAlloc)/1024/1024)
// 	fmt.Printf("GC runs: %v\n", uintCurrent.UintNumGC)
// 	fmt.Println()
// }

// func main() {
// 	// Seed the random number generator
// 	rand.Seed(time.Now().UnixNano())

// 	// Number of elements and sets
// 	const uintNumElements = 20_000_000

// 	// Track memory usage
// 	uintStartStats := UintGetMemoryStats()
// 	fmt.Println("Starting memory monitoring...")
// 	UintPrintMemoryUsage("Initial", uintStartStats, uintStartStats)

// 	// Create sets
// 	uintUF := NewUintUnionFind()

// 	// Generate our elements first
// 	uintElements := make([]UintElement, uintNumElements)

// 	fmt.Println("Generating elements...")
// 	uintPreGenStats := UintGetMemoryStats()
// 	for uintI := 0; uintI < uintNumElements; uintI++ {
// 		uintElements[uintI] = UintGenerateRandomElement()
// 	}
// 	uintGenStats := UintGetMemoryStats()
// 	UintPrintMemoryUsage("After generating elements", uintPreGenStats, uintGenStats)

// 	// Create the sets
// 	fmt.Println("Creating initial sets...")
// 	uintPreMakeSetStats := UintGetMemoryStats()
// 	for uintI := 0; uintI < uintNumElements; uintI++ {
// 		uintUF.UintMakeSet(uintElements[uintI])

// 		// Print progress periodically
// 		if (uintI+1)%1_000_000 == 0 {
// 			fmt.Printf("Created %d sets...\n", uintI+1)
// 			uintCurrentStats := UintGetMemoryStats()
// 			UintPrintMemoryUsage(fmt.Sprintf("After creating %d sets", uintI+1), uintPreMakeSetStats, uintCurrentStats)
// 		}
// 	}

// 	uintPostMakeSetStats := UintGetMemoryStats()
// 	UintPrintMemoryUsage("After creating all sets", uintPreMakeSetStats, uintPostMakeSetStats)

// 	// Now perform unions to create sets of sizes 1-4
// 	// 80% of sets will remain size 1, 20% will be merged into sets of size 2-4

// 	fmt.Println("Performing unions to create larger sets...")
// 	uintPreUnionStats := UintGetMemoryStats()

// 	// Calculate how many elements to merge (20% of total)
// 	uintElemsToMerge := int(float64(uintNumElements) * 0.2)

// 	// Track how many have been merged so far
// 	uintMergedCount := 0

// 	// Track sets by target size
// 	uintTargetSets := make(map[int][]UintElement)

// 	// Initialize with sizes 2, 3, 4
// 	for uintSize := 2; uintSize <= 4; uintSize++ {
// 		uintTargetSets[uintSize] = make([]UintElement, 0)
// 	}

// 	// Assign elements to target set sizes (determine which elements go into which size sets)
// 	for uintI := 0; uintI < uintElemsToMerge; uintI++ {
// 		// Select a random size between 2-4, with equal probability
// 		uintTargetSize := rand.Intn(3) + 2
// 		uintTargetSets[uintTargetSize] = append(uintTargetSets[uintTargetSize], uintElements[uintI])
// 	}

// 	// Create the sets of different sizes
// 	for uintSize := 2; uintSize <= 4; uintSize++ {
// 		uintTargetElems := uintTargetSets[uintSize]

// 		fmt.Printf("Creating sets of size %d...\n", uintSize)

// 		// Group elements for sets of this size
// 		uintNumSets := len(uintTargetElems) / uintSize
// 		for uintI := 0; uintI < uintNumSets; uintI++ {
// 			// Base element becomes the "root" of this set
// 			uintBaseElem := uintTargetElems[uintI*uintSize]

// 			// Merge other elements into this set
// 			for uintJ := 1; uintJ < uintSize; uintJ++ {
// 				if uintI*uintSize+uintJ < len(uintTargetElems) {
// 					uintElem := uintTargetElems[uintI*uintSize+uintJ]
// 					uintUF.UintUnion(uintBaseElem, uintElem)
// 					uintMergedCount++

// 					// Print memory stats periodically
// 					if uintMergedCount%1_000_000 == 0 {
// 						fmt.Printf("Merged %d elements...\n", uintMergedCount)
// 						uintCurrentStats := UintGetMemoryStats()
// 						UintPrintMemoryUsage(fmt.Sprintf("After %d unions", uintMergedCount), uintPreUnionStats, uintCurrentStats)
// 					}
// 				}
// 			}
// 		}
// 	}

// 	uintPostUnionStats := UintGetMemoryStats()
// 	UintPrintMemoryUsage("After all unions", uintPreUnionStats, uintPostUnionStats)

// 	// Verify the distribution of set sizes
// 	uintSizeCounts := make(map[int]int)

// 	fmt.Println("Verifying set size distribution...")
// 	for _, uintElem := range uintElements {
// 		uintRoot := uintUF.UintFind(uintElem)
// 		if uintRoot != nil {
// 			uintSizeCounts[uintRoot.UintSize]++
// 		}
// 	}

// 	// Print the counts of sets of each size
// 	fmt.Println("Set size distribution:")
// 	for uintSize := 1; uintSize <= 4; uintSize++ {
// 		fmt.Printf("Sets of size %d: %d\n", uintSize, uintSizeCounts[uintSize])
// 	}

// 	// Final memory stats
// 	uintFinalStats := UintGetMemoryStats()
// 	UintPrintMemoryUsage("Final", uintStartStats, uintFinalStats)
// }
