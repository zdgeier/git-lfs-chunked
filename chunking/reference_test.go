package chunking

// rustReferenceLengths are the chunk lengths the Rust fastcdc crate (4.0.1,
// v2020, normalization level 1, seed 0) produces for the input described in
// TestFastCDCMatchesRustReference, with min/avg/max = 1024/4096/16384.
var rustReferenceLengths = []int{5115, 9757, 1375, 6102, 12407, 1375, 6102, 5105, 7302, 1375, 6102, 3419}
