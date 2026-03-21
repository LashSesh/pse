//! Spectral-guided pattern space navigator for PSE (C29).
//!
//! Explores the design space using golden-angle spirals and spectral signatures
//! (quality, stability, efficiency) to guide search toward high-resonance regions.

// isls-navigator: C29 — Spectral-Guided Pattern Space Explorer
// TRITON spirals. The mesh triangulates. The Laplacian reads the topology.
// The gradient pulls the spiral toward gold.
//
// metatron_triton is not yet available as a path dependency.
// NavigatorSpiral provides the golden-angle spiral fallback.

use serde::{Deserialize, Serialize};

// ─── SpectralSignature ────────────────────────────────────────────────────────
// Local definition (metatron_triton not yet available).

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct SpectralSignature {
    pub psi: f64,   // quality (Ψ)
    pub rho: f64,   // stability (ρ)
    pub omega: f64, // efficiency (ω)
}

impl SpectralSignature {
    pub fn new(psi: f64, rho: f64, omega: f64) -> Self {
        Self { psi, rho, omega }
    }

    pub fn resonance(&self) -> f64 {
        self.psi * self.rho * self.omega
    }

    pub fn zero() -> Self {
        Self { psi: 0.0, rho: 0.0, omega: 0.0 }
    }
}

// ─── Mesh Types ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MeshVertex {
    pub id: usize,
    pub point: Vec<f64>,
    pub signature: SpectralSignature,
    pub resonance: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MeshEdge {
    pub v1: usize,
    pub v2: usize,
    pub weight: f64,
    pub distance: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Simplex {
    pub vertices: Vec<usize>,
}

// ─── SimplexMesh ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SimplexMesh {
    pub vertices: Vec<MeshVertex>,
    pub edges: Vec<MeshEdge>,
    pub simplices: Vec<Simplex>,
}

impl SimplexMesh {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn vertex_count(&self) -> usize {
        self.vertices.len()
    }

    pub fn add_vertex(&mut self, point: &[f64], signature: &SpectralSignature) -> usize {
        let id = self.vertices.len();
        let resonance = signature.resonance();
        self.vertices.push(MeshVertex {
            id,
            point: point.to_vec(),
            signature: signature.clone(),
            resonance,
        });
        id
    }

    fn euclidean_distance(a: &[f64], b: &[f64]) -> f64 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f64>()
            .sqrt()
    }

    pub fn edge_exists(&self, v1: usize, v2: usize) -> bool {
        self.edges
            .iter()
            .any(|e| (e.v1 == v1 && e.v2 == v2) || (e.v1 == v2 && e.v2 == v1))
    }

    /// Connect vertex_id to its k nearest neighbors; return newly added edges.
    pub fn connect_knn(&mut self, vertex_id: usize, k: usize) -> Vec<MeshEdge> {
        let point = self.vertices[vertex_id].point.clone();
        let mut distances: Vec<(usize, f64)> = self
            .vertices
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != vertex_id)
            .map(|(i, v)| (i, Self::euclidean_distance(&point, &v.point)))
            .collect();
        distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut new_edges = Vec::new();
        for (neighbor_id, dist) in distances.into_iter().take(k) {
            if !self.edge_exists(vertex_id, neighbor_id) {
                let edge = MeshEdge {
                    v1: vertex_id,
                    v2: neighbor_id,
                    weight: 0.0,
                    distance: dist,
                };
                self.edges.push(edge.clone());
                new_edges.push(edge);
            }
        }
        new_edges
    }

    /// Weight = geometric mean of endpoint resonances / distance.
    /// High-resonance close pairs get the highest weight.
    pub fn weight_edges_by_resonance(&mut self, edges: &[MeshEdge]) {
        for edge in edges {
            let r1 = self.vertices[edge.v1].resonance;
            let r2 = self.vertices[edge.v2].resonance;
            let geo_mean = (r1 * r2).sqrt();
            let weight = geo_mean / (edge.distance + 1e-10);
            if let Some(e) = self.edges.iter_mut().find(|e| {
                (e.v1 == edge.v1 && e.v2 == edge.v2) || (e.v1 == edge.v2 && e.v2 == edge.v1)
            }) {
                e.weight = weight;
            }
        }
    }

    /// Detect triangle simplices involving any of the given edges.
    pub fn detect_simplices(&self, new_edges: &[MeshEdge]) -> Vec<Simplex> {
        let mut simplices = Vec::new();
        let mut seen: std::collections::BTreeSet<Vec<usize>> = std::collections::BTreeSet::new();

        for edge in new_edges {
            let a = edge.v1;
            let b = edge.v2;

            let neighbors_a: std::collections::BTreeSet<usize> = self
                .edges
                .iter()
                .filter(|e| e.v1 == a || e.v2 == a)
                .map(|e| if e.v1 == a { e.v2 } else { e.v1 })
                .collect();
            let neighbors_b: std::collections::BTreeSet<usize> = self
                .edges
                .iter()
                .filter(|e| e.v1 == b || e.v2 == b)
                .map(|e| if e.v1 == b { e.v2 } else { e.v1 })
                .collect();

            for &c in neighbors_a.intersection(&neighbors_b) {
                let mut key = vec![a, b, c];
                key.sort_unstable();
                if seen.insert(key.clone()) {
                    simplices.push(Simplex { vertices: key });
                }
            }
        }
        simplices
    }

    pub fn add_simplices(&mut self, simplices: &[Simplex]) {
        for s in simplices {
            let mut key = s.vertices.clone();
            key.sort_unstable();
            let exists = self.simplices.iter().any(|existing| {
                let mut ek = existing.vertices.clone();
                ek.sort_unstable();
                ek == key
            });
            if !exists {
                self.simplices.push(s.clone());
            }
        }
    }

    pub fn remove_simplices(&mut self, simplices: &[Simplex]) {
        for s in simplices {
            let mut key = s.vertices.clone();
            key.sort_unstable();
            self.simplices.retain(|existing| {
                let mut ek = existing.vertices.clone();
                ek.sort_unstable();
                ek != key
            });
        }
    }

    // ─── Betti Numbers ────────────────────────────────────────────────────────

    fn connected_components(&self) -> usize {
        if self.vertices.is_empty() {
            return 0;
        }
        let n = self.vertices.len();
        let mut parent: Vec<usize> = (0..n).collect();

        fn find(parent: &mut Vec<usize>, x: usize) -> usize {
            if parent[x] != x {
                let root = find(parent, parent[x]);
                parent[x] = root;
            }
            parent[x]
        }

        for edge in &self.edges {
            let px = find(&mut parent, edge.v1);
            let py = find(&mut parent, edge.v2);
            if px != py {
                parent[px] = py;
            }
        }

        let mut roots = std::collections::BTreeSet::new();
        for i in 0..n {
            roots.insert(find(&mut parent, i));
        }
        roots.len()
    }

    /// Lightweight Betti numbers without external libraries (~50 lines).
    /// Returns [b0, b1, b2].
    pub fn betti_numbers(&self) -> Vec<usize> {
        let v = self.vertices.len();
        let e = self.edges.len();
        let f = self.simplices.iter().filter(|s| s.vertices.len() == 3).count();
        let t = self.simplices.iter().filter(|s| s.vertices.len() == 4).count();

        let b0 = if v == 0 { 0 } else { self.connected_components() };
        let chi = v as i64 - e as i64 + f as i64 - t as i64;
        let b1 = (e as i64 - v as i64 + b0 as i64).max(0) as usize;
        let b2 = (chi - b0 as i64 + b1 as i64).max(0) as usize;

        vec![b0, b1, b2]
    }

    // ─── Laplacian & Spectral Analysis ────────────────────────────────────────

    /// Build a SparseLaplacian directly from the mesh graph.
    /// This feeds into C16's `spectral_decompose` without needing PersistentGraph.
    pub fn laplacian_matrix(&self) -> pse_topology::SparseLaplacian {
        let n = self.vertices.len();
        let mut degree = vec![0.0f64; n];
        let mut adjacency = Vec::new();

        for edge in &self.edges {
            let w = edge.weight.max(0.0);
            adjacency.push((edge.v1, edge.v2, w));
            adjacency.push((edge.v2, edge.v1, w));
            degree[edge.v1] += w;
            degree[edge.v2] += w;
        }

        pse_topology::SparseLaplacian { n, degree, adjacency }
    }

    /// Spectral gap λ₁ via C16 decomposition.
    pub fn spectral_gap(&self, laplacian: &pse_topology::SparseLaplacian) -> f64 {
        if laplacian.n < 2 {
            return 0.0;
        }
        let k = laplacian.n.min(10);
        pse_topology::spectral_decompose(laplacian, k).spectral_gap
    }

    /// Spectral gradient: direction from vertex toward the Fiedler-opposite cluster.
    /// This is the "invisible string" — the Fiedler vector tells TRITON where to go.
    #[allow(clippy::needless_range_loop)]
    pub fn spectral_gradient(
        &self,
        vertex_id: usize,
        laplacian: &pse_topology::SparseLaplacian,
    ) -> Vec<f64> {
        let dim = self.vertices.get(vertex_id).map(|v| v.point.len()).unwrap_or(1);
        if laplacian.n < 2 || self.vertices.is_empty() {
            return vec![0.0; dim];
        }

        let k = laplacian.n.min(10);
        let decomp = pse_topology::spectral_decompose(laplacian, k);
        let fiedler = &decomp.fiedler_vector;

        if fiedler.len() <= vertex_id {
            return vec![0.0; dim];
        }
        let vertex_fiedler = fiedler[vertex_id];

        let target_cluster: Vec<usize> = fiedler
            .iter()
            .enumerate()
            .filter(|(_, &f)| (f > 0.0) != (vertex_fiedler > 0.0))
            .map(|(i, _)| i)
            .filter(|&i| i < self.vertices.len())
            .collect();

        if target_cluster.is_empty() {
            return vec![0.0; dim];
        }

        let mut centroid = vec![0.0f64; dim];
        for &tid in &target_cluster {
            let pt = &self.vertices[tid].point;
            for d in 0..dim.min(pt.len()) {
                centroid[d] += pt[d];
            }
        }
        let len = target_cluster.len() as f64;
        let origin = &self.vertices[vertex_id].point;
        for d in 0..dim {
            centroid[d] = centroid[d] / len - origin.get(d).copied().unwrap_or(0.0);
        }

        let norm: f64 = centroid.iter().map(|x| x * x).sum::<f64>().sqrt();
        if norm > 1e-10 {
            centroid.iter_mut().for_each(|x| *x /= norm);
        }
        centroid
    }

    // ─── Local Entropy ────────────────────────────────────────────────────────

    pub fn neighbors_of(&self, vertex_id: usize) -> Vec<usize> {
        self.edges
            .iter()
            .filter(|e| e.v1 == vertex_id || e.v2 == vertex_id)
            .map(|e| if e.v1 == vertex_id { e.v2 } else { e.v1 })
            .collect()
    }

    /// Shannon entropy of the resonance distribution in the k-neighborhood.
    pub fn local_entropy(&self, vertex_id: usize) -> f64 {
        let neighbors = self.neighbors_of(vertex_id);
        if neighbors.is_empty() {
            return 0.0;
        }

        let resonances: Vec<f64> = neighbors
            .iter()
            .map(|&n| self.vertices[n].resonance)
            .collect();
        let total: f64 = resonances.iter().sum();
        if total < 1e-10 {
            return 0.0;
        }

        -resonances
            .iter()
            .map(|r| r / total)
            .filter(|&p| p > 1e-10)
            .map(|p| p * p.ln())
            .sum::<f64>()
    }

    // ─── Singularity Detection ────────────────────────────────────────────────

    /// Singularity: vertex with resonance > 2σ above its neighborhood mean.
    pub fn detect_singularities(&self) -> Vec<usize> {
        self.vertices
            .iter()
            .enumerate()
            .filter(|(id, v)| {
                let neighbors = self.neighbors_of(*id);
                if neighbors.len() < 3 {
                    return false;
                }
                let mean: f64 = neighbors
                    .iter()
                    .map(|&n| self.vertices[n].resonance)
                    .sum::<f64>()
                    / neighbors.len() as f64;
                let variance: f64 = neighbors
                    .iter()
                    .map(|&n| (self.vertices[n].resonance - mean).powi(2))
                    .sum::<f64>()
                    / neighbors.len() as f64;
                let sigma = variance.sqrt();
                v.resonance > mean + 2.0 * sigma
            })
            .map(|(id, _)| id)
            .collect()
    }
}

// ─── Topology Guard ───────────────────────────────────────────────────────────

/// Returns true iff no Betti number changed by more than `tolerance`.
pub fn topology_stable(before: &[usize], after: &[usize], tolerance: usize) -> bool {
    before
        .iter()
        .zip(after.iter())
        .all(|(b, a)| (*b as i64 - *a as i64).unsigned_abs() as usize <= tolerance)
}

// ─── NavigatorSpiral (TRITON fallback) ────────────────────────────────────────
// Golden-angle spiral in n-dimensional [0,1]^n with momentum bias.

const GOLDEN_ANGLE: f64 = std::f64::consts::TAU * 0.618_033_988_749_895;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NavigatorSpiral {
    pub dim: usize,
    pub radius: f64,
    angle: f64,
    momentum: Vec<f64>,
    step_count: usize,
    #[allow(dead_code)]
    seed: u64,
    best_resonance: f64,
    best_point: Option<Vec<f64>>,
    best_signature: Option<SpectralSignature>,
}

impl NavigatorSpiral {
    pub fn new(dim: usize, seed: u64) -> Self {
        Self {
            dim,
            radius: 0.3,
            angle: 0.0,
            momentum: vec![0.0; dim],
            step_count: 0,
            seed,
            best_resonance: 0.0,
            best_point: None,
            best_signature: None,
        }
    }

    /// Generate the next candidate point in `[0,1]^dim` via golden-angle spiral.
    #[allow(clippy::needless_range_loop)]
    pub fn next_point(&mut self) -> Vec<f64> {
        self.step_count += 1;
        self.angle += GOLDEN_ANGLE;

        let mut point = vec![0.5f64; self.dim];

        // First two dimensions: golden-angle circle
        if self.dim >= 2 {
            let (sin_a, cos_a) = self.angle.sin_cos();
            point[0] = (0.5 + self.radius * cos_a).clamp(0.0, 1.0);
            point[1] = (0.5 + self.radius * sin_a).clamp(0.0, 1.0);
        } else if self.dim == 1 {
            point[0] = (0.5 + self.radius * self.angle.sin()).clamp(0.0, 1.0);
        }

        // Higher dimensions: Halton-like angular decomposition
        for d in 2..self.dim {
            let base = (d as f64 + 1.0).recip();
            point[d] = (0.5 + self.radius * (self.angle * base).sin()).clamp(0.0, 1.0);
        }

        // Apply momentum bias
        for d in 0..self.dim {
            point[d] = (point[d] + 0.1 * self.momentum[d]).clamp(0.0, 1.0);
        }

        point
    }

    /// Update momentum from spectral gradient and normalized reward.
    #[allow(clippy::needless_range_loop)]
    pub fn update_momentum(&mut self, gradient: &[f64], reward: f64) {
        let alpha = (0.3 * reward).clamp(0.0, 0.6);
        for d in 0..self.dim.min(gradient.len()) {
            self.momentum[d] = 0.7 * self.momentum[d] + alpha * gradient[d];
        }
    }

    /// High entropy → explore (increase radius). Low entropy → exploit (decrease).
    pub fn adapt_radius(&mut self, entropy: f64) {
        let target = entropy.clamp(0.0, 3.0) / 3.0;
        self.radius = (self.radius * 0.9 + target * 0.1).clamp(0.05, 0.5);
    }

    pub fn record_result(&mut self, point: Vec<f64>, signature: SpectralSignature) {
        let res = signature.resonance();
        if res > self.best_resonance {
            self.best_resonance = res;
            self.best_point = Some(point);
            self.best_signature = Some(signature);
        }
    }

    pub fn best_signature(&self) -> Option<&SpectralSignature> {
        self.best_signature.as_ref()
    }

    pub fn best_resonance(&self) -> f64 {
        self.best_resonance
    }

    pub fn best_point(&self) -> Option<&Vec<f64>> {
        self.best_point.as_ref()
    }
}

// ─── Navigator Config ─────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NavigatorConfig {
    pub k: usize,
    pub allow_betti_change: bool,
    pub betti_tolerance: usize,
    pub min_vertices_for_guard: usize,
    pub dim: usize,
    pub seed: u64,
}

impl Default for NavigatorConfig {
    fn default() -> Self {
        Self {
            k: 3,
            allow_betti_change: false,
            betti_tolerance: 1,
            min_vertices_for_guard: 5,
            dim: 5,
            seed: 42,
        }
    }
}

// ─── Exploration Step ─────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExplorationStep {
    pub index: usize,
    pub point: Vec<f64>,
    pub signature: SpectralSignature,
    pub vertex_id: usize,
    pub edges_added: usize,
    pub simplices_added: usize,
    pub spectral_gap: f64,
    pub local_entropy: f64,
    pub betti: Vec<usize>,
    pub best_resonance: f64,
}

// ─── Navigator ────────────────────────────────────────────────────────────────

pub struct Navigator<E>
where
    E: Fn(&[f64]) -> SpectralSignature,
{
    pub spiral: NavigatorSpiral,
    pub mesh: SimplexMesh,
    pub config: NavigatorConfig,
    pub history: Vec<ExplorationStep>,
    evaluator: E,
}

impl<E: Fn(&[f64]) -> SpectralSignature> Navigator<E> {
    pub fn new(config: NavigatorConfig, evaluator: E) -> Self {
        let spiral = NavigatorSpiral::new(config.dim, config.seed);
        Self {
            spiral,
            mesh: SimplexMesh::new(),
            config,
            history: Vec::new(),
            evaluator,
        }
    }

    /// Execute one exploration step (spec §2 — The Exploration Loop).
    pub fn step(&mut self) -> ExplorationStep {
        // 1. Generate next candidate point
        let point = self.spiral.next_point();

        // 2. Evaluate via injected evaluator
        let signature = (self.evaluator)(&point);

        // 3. Add as mesh vertex
        let vertex_id = self.mesh.add_vertex(&point, &signature);

        // 4. Connect k nearest neighbors
        let new_edges = self.mesh.connect_knn(vertex_id, self.config.k);

        // 5. Weight edges by resonance
        self.mesh.weight_edges_by_resonance(&new_edges);

        // 6. Detect new triangle simplices
        let new_simplices = self.mesh.detect_simplices(&new_edges);

        // 7. Topology guard
        let betti_before = self.mesh.betti_numbers();
        self.mesh.add_simplices(&new_simplices);
        let betti_after = self.mesh.betti_numbers();

        let simplices_added =
            if !self.config.allow_betti_change
                && !topology_stable(&betti_before, &betti_after, self.config.betti_tolerance)
                && self.mesh.vertex_count() > self.config.min_vertices_for_guard
            {
                self.mesh.remove_simplices(&new_simplices);
                0
            } else {
                new_simplices.len()
            };

        // 8. Compute Laplacian on the mesh
        let laplacian = self.mesh.laplacian_matrix();

        // 9. Spectral gradient → TRITON momentum bias
        let gradient = self.mesh.spectral_gradient(vertex_id, &laplacian);

        // 10. Momentum update (reward = current / best resonance)
        let best_res = self
            .spiral
            .best_signature()
            .map(|s| s.resonance())
            .unwrap_or(1.0)
            .max(1e-10);
        let reward = signature.resonance() / best_res;
        self.spiral.update_momentum(&gradient, reward);

        // 11. Entropy-driven radius adaptation
        let local_entropy = self.mesh.local_entropy(vertex_id);
        self.spiral.adapt_radius(local_entropy);

        // 12. Record best
        self.spiral.record_result(point.clone(), signature.clone());

        // 13. Collect metrics for step record
        let spectral_gap = self.mesh.spectral_gap(&laplacian);
        let betti = self.mesh.betti_numbers();

        let step = ExplorationStep {
            index: self.history.len(),
            point,
            signature,
            vertex_id,
            edges_added: new_edges.len(),
            simplices_added,
            spectral_gap,
            local_entropy,
            betti,
            best_resonance: self.spiral.best_resonance(),
        };
        self.history.push(step.clone());
        step
    }

    /// Run n steps and return all steps.
    pub fn run(&mut self, n: usize) -> Vec<ExplorationStep> {
        (0..n).map(|_| self.step()).collect()
    }

    pub fn best_signature(&self) -> Option<&SpectralSignature> {
        self.spiral.best_signature()
    }

    pub fn singularities(&self) -> Vec<usize> {
        self.mesh.detect_singularities()
    }

    /// Serialize the current mesh to pretty JSON.
    pub fn export_mesh(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self.mesh)
    }
}

// ─── Persistent Navigator State (for CLI) ────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NavigatorState {
    pub mesh: SimplexMesh,
    pub best_resonance: f64,
    pub best_point: Option<Vec<f64>>,
    pub best_signature: Option<SpectralSignature>,
    pub mode: String,
    pub steps_run: usize,
}

impl NavigatorState {
    pub fn new(mode: String) -> Self {
        Self {
            mesh: SimplexMesh::new(),
            best_resonance: 0.0,
            best_point: None,
            best_signature: None,
            mode,
            steps_run: 0,
        }
    }

    pub fn from_navigator<E: Fn(&[f64]) -> SpectralSignature>(
        nav: &Navigator<E>,
        mode: String,
    ) -> Self {
        Self {
            mesh: nav.mesh.clone(),
            best_resonance: nav.spiral.best_resonance(),
            best_point: nav.spiral.best_point().cloned(),
            best_signature: nav.best_signature().cloned(),
            mode,
            steps_run: nav.history.len(),
        }
    }

    pub fn save(&self, path: &std::path::Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self)
            .map_err(std::io::Error::other)?;
        std::fs::write(path, json)
    }

    pub fn load(path: &std::path::Path) -> std::io::Result<Self> {
        let json = std::fs::read_to_string(path)?;
        serde_json::from_str(&json)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}

// ─── Tests (AT-NV1..AT-NV12) ─────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // AT-NV1: Add 10 vertices; verify mesh has 10 vertices.
    #[test]
    fn at_nv1_mesh_vertex_add() {
        let mut mesh = SimplexMesh::new();
        let sig = SpectralSignature::new(0.5, 0.5, 0.5);
        for i in 0..10 {
            mesh.add_vertex(&[i as f64, 0.0], &sig);
        }
        assert_eq!(mesh.vertex_count(), 10);
    }

    // AT-NV2: Add 5 vertices with k=2; verify each has ≤ 2 outgoing edges.
    #[test]
    fn at_nv2_knn_edges() {
        let mut mesh = SimplexMesh::new();
        let sig = SpectralSignature::new(0.5, 0.5, 0.5);
        for i in 0..5 {
            let v = mesh.add_vertex(&[i as f64, 0.0], &sig);
            mesh.connect_knn(v, 2);
        }
        for v in 0..5usize {
            let outgoing = mesh.edges.iter().filter(|e| e.v1 == v).count();
            assert!(outgoing <= 2, "vertex {} has {} outgoing edges", v, outgoing);
        }
    }

    // AT-NV3: High-res edge has higher weight than low-res edge.
    #[test]
    fn at_nv3_resonance_weight() {
        let mut mesh = SimplexMesh::new();
        let high = SpectralSignature::new(0.9, 0.9, 0.9); // resonance = 0.729
        let low = SpectralSignature::new(0.1, 0.1, 0.1);  // resonance = 0.001

        let h0 = mesh.add_vertex(&[0.0, 0.0], &high);
        let h1 = mesh.add_vertex(&[1.0, 0.0], &high);
        let l0 = mesh.add_vertex(&[0.0, 10.0], &low);
        let _l1 = mesh.add_vertex(&[1.0, 10.0], &low);

        let mut edges = mesh.connect_knn(h0, 1);
        edges.extend(mesh.connect_knn(l0, 1));
        mesh.weight_edges_by_resonance(&edges);

        let high_w = mesh.edges.iter()
            .find(|e| (e.v1 == h0 && e.v2 == h1) || (e.v1 == h1 && e.v2 == h0))
            .map(|e| e.weight).unwrap_or(0.0);
        let low_w = mesh.edges.iter()
            .find(|e| e.v1 == l0 || e.v2 == l0)
            .map(|e| e.weight).unwrap_or(0.0);

        assert!(high_w > low_w, "high={} should be > low={}", high_w, low_w);
    }

    // AT-NV4: 3 mutually connected vertices → 1 triangle simplex.
    #[test]
    fn at_nv4_simplex_detection() {
        let mut mesh = SimplexMesh::new();
        let sig = SpectralSignature::new(0.5, 0.5, 0.5);
        mesh.add_vertex(&[0.0, 0.0], &sig);
        mesh.add_vertex(&[1.0, 0.0], &sig);
        mesh.add_vertex(&[0.5, 1.0], &sig);

        let all_edges = vec![
            MeshEdge { v1: 0, v2: 1, weight: 0.0, distance: 1.0 },
            MeshEdge { v1: 1, v2: 2, weight: 0.0, distance: 1.0 },
            MeshEdge { v1: 0, v2: 2, weight: 0.0, distance: 1.0 },
        ];
        mesh.edges.extend(all_edges.clone());

        let simplices = mesh.detect_simplices(&all_edges);
        assert_eq!(simplices.len(), 1, "expected 1 triangle, got {}", simplices.len());
        assert_eq!(simplices[0].vertices.len(), 3);
    }

    // AT-NV5: 4-vertex cycle → b0=1, b1=1.
    #[test]
    fn at_nv5_betti_numbers() {
        let mut mesh = SimplexMesh::new();
        let sig = SpectralSignature::new(0.5, 0.5, 0.5);
        for i in 0..4 {
            mesh.add_vertex(&[i as f64, 0.0], &sig);
        }
        mesh.edges.push(MeshEdge { v1: 0, v2: 1, weight: 1.0, distance: 1.0 });
        mesh.edges.push(MeshEdge { v1: 1, v2: 2, weight: 1.0, distance: 1.0 });
        mesh.edges.push(MeshEdge { v1: 2, v2: 3, weight: 1.0, distance: 1.0 });
        mesh.edges.push(MeshEdge { v1: 3, v2: 0, weight: 1.0, distance: 1.0 });

        let betti = mesh.betti_numbers();
        assert_eq!(betti[0], 1, "b0={} (expected 1 component)", betti[0]);
        assert_eq!(betti[1], 1, "b1={} (expected 1 cycle)", betti[1]);
    }

    // AT-NV6: Topology guard rejects Betti-destabilizing updates.
    #[test]
    fn at_nv6_topology_guard() {
        let before = vec![1usize, 0, 0];
        // Δb1 = 2 → unstable (tolerance = 1)
        assert!(!topology_stable(&before, &[1, 2, 0], 1));
        // Δb1 = 1 → stable (within tolerance)
        assert!(topology_stable(&before, &[1, 1, 0], 1));
        // Exact match → stable
        assert!(topology_stable(&before, &[1, 0, 0], 1));
    }

    // AT-NV7: Gradient at cluster A vertex points toward cluster B.
    #[test]
    fn at_nv7_spectral_gradient() {
        let mut mesh = SimplexMesh::new();
        let sig = SpectralSignature::new(0.5, 0.5, 0.5);

        // Cluster A (x ≈ 0)
        let v0 = mesh.add_vertex(&[0.0, 0.0], &sig);
        let v1 = mesh.add_vertex(&[0.1, 0.0], &sig);
        // Cluster B (x ≈ 1)
        let v2 = mesh.add_vertex(&[1.0, 0.0], &sig);
        let v3 = mesh.add_vertex(&[1.1, 0.0], &sig);

        // Strong within-cluster edges, weak between-cluster edge
        mesh.edges.push(MeshEdge { v1: v0, v2: v1, weight: 5.0, distance: 0.1 });
        mesh.edges.push(MeshEdge { v1: v2, v2: v3, weight: 5.0, distance: 0.1 });
        mesh.edges.push(MeshEdge { v1: v1, v2: v2, weight: 0.1, distance: 0.9 });

        let laplacian = mesh.laplacian_matrix();
        let gradient = mesh.spectral_gradient(v0, &laplacian);

        assert!(!gradient.is_empty());
        // Gradient should have positive x-component (toward cluster B at x≈1)
        assert!(
            gradient[0] > 0.0,
            "gradient[0]={:.4} should be positive (toward cluster B)",
            gradient[0]
        );
    }

    // AT-NV8: Uniform neighborhood → higher entropy than dominant-neighbor mesh.
    #[test]
    fn at_nv8_local_entropy() {
        // Mesh 1: uniform resonance → high entropy
        let mut m1 = SimplexMesh::new();
        let uniform = SpectralSignature::new(0.5, 0.5, 0.5);
        let c1 = m1.add_vertex(&[0.0, 0.0], &uniform);
        for i in 1..=4 {
            m1.add_vertex(&[i as f64, 0.0], &uniform);
            m1.edges.push(MeshEdge { v1: c1, v2: i, weight: 1.0, distance: 1.0 });
        }
        let h_uniform = m1.local_entropy(c1);

        // Mesh 2: one dominant neighbor → low entropy
        let mut m2 = SimplexMesh::new();
        let c2 = m2.add_vertex(&[0.0, 0.0], &SpectralSignature::new(0.5, 0.5, 0.5));
        m2.add_vertex(&[1.0, 0.0], &SpectralSignature::new(0.99, 0.99, 0.99));
        m2.add_vertex(&[2.0, 0.0], &SpectralSignature::new(0.01, 0.01, 0.01));
        m2.add_vertex(&[3.0, 0.0], &SpectralSignature::new(0.01, 0.01, 0.01));
        m2.edges.push(MeshEdge { v1: c2, v2: 1, weight: 1.0, distance: 1.0 });
        m2.edges.push(MeshEdge { v1: c2, v2: 2, weight: 1.0, distance: 1.0 });
        m2.edges.push(MeshEdge { v1: c2, v2: 3, weight: 1.0, distance: 1.0 });
        let h_dominant = m2.local_entropy(c2);

        assert!(
            h_uniform > h_dominant,
            "uniform entropy ({:.4}) should exceed dominant entropy ({:.4})",
            h_uniform, h_dominant
        );
    }

    // AT-NV9: 20 navigator steps; best resonance > 0 and monotonically non-decreasing.
    #[test]
    fn at_nv9_triton_integration() {
        let config = NavigatorConfig { dim: 2, k: 3, seed: 42, ..Default::default() };
        let mut nav = Navigator::new(config, |params: &[f64]| {
            // Peak resonance at (1, 1)
            let dist = ((params[0] - 1.0).powi(2) + (params[1] - 1.0).powi(2)).sqrt();
            let r = (1.0 - dist).max(0.0);
            SpectralSignature::new(r, r, r)
        });

        let steps = nav.run(20);
        assert_eq!(steps.len(), 20);
        assert!(nav.spiral.best_resonance() > 0.0, "best resonance should be > 0");

        // best_resonance is non-decreasing step-by-step
        let mut prev_best = 0.0f64;
        for step in &steps {
            assert!(step.best_resonance >= prev_best - 1e-12);
            prev_best = step.best_resonance;
        }
    }

    // AT-NV10: Vertex with resonance > 2σ above neighbors is detected as singularity.
    #[test]
    fn at_nv10_singularity_detection() {
        let mut mesh = SimplexMesh::new();
        let gold = SpectralSignature::new(0.99, 0.99, 0.99);
        let low = SpectralSignature::new(0.01, 0.01, 0.01);

        let center = mesh.add_vertex(&[5.0, 5.0], &gold);
        for i in 0..5 {
            let neighbor = mesh.add_vertex(&[i as f64, 0.0], &low);
            mesh.edges.push(MeshEdge { v1: center, v2: neighbor, weight: 1.0, distance: 1.0 });
        }

        let sings = mesh.detect_singularities();
        assert!(sings.contains(&center), "center should be a singularity (got {:?})", sings);
    }

    // AT-NV11: Same seed → identical mesh and history.
    #[test]
    fn at_nv11_determinism() {
        let make = || {
            let config = NavigatorConfig { dim: 2, k: 2, seed: 12345, ..Default::default() };
            Navigator::new(config, |params: &[f64]| {
                SpectralSignature::new(params[0], params[0], params[0])
            })
        };
        let mut n1 = make();
        let mut n2 = make();
        let s1 = n1.run(10);
        let s2 = n2.run(10);

        for (a, b) in s1.iter().zip(s2.iter()) {
            assert_eq!(a.point, b.point, "points differ at step {}", a.index);
            assert_eq!(a.edges_added, b.edges_added);
            assert_eq!(a.betti, b.betti);
        }
    }

    // AT-NV12: Navigator fallback (no metatron_triton) stays in [0,1]^n.
    #[test]
    fn at_nv12_fallback_without_triton() {
        let config = NavigatorConfig { dim: 3, k: 2, seed: 99, ..Default::default() };
        let mut nav = Navigator::new(config, |params: &[f64]| {
            SpectralSignature::new(params[0], params[1], params[2])
        });
        let steps = nav.run(5);
        assert_eq!(steps.len(), 5);
        for step in &steps {
            for &x in &step.point {
                assert!(
                    (0.0..=1.0).contains(&x),
                    "coordinate {} out of [0, 1]", x
                );
            }
        }
    }
}
