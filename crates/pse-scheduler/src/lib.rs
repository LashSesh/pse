//! Spiral scheduler for PSE (C15).
//!
//! Provides adaptive tick granularity that adjusts sub-step counts based on
//! system pressure metrics (drift, flux, synchronization).

use pse_types::SchedulerConfig;

// ─── Schedule Strategy ────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub enum ScheduleStrategy {
    /// n_k = clamp(floor(n_min + (n_max - n_min) * max(d, F, S)), n_min, n_max)
    MaxPressure,
    /// Weighted combination of d, F, S
    Weighted { w_d: f64, w_f: f64, w_s: f64 },
    /// Constant sub-steps regardless of metrics
    Fixed(u32),
}

impl ScheduleStrategy {
    pub fn from_config(config: &SchedulerConfig) -> Self {
        match config.strategy.as_str() {
            "weighted" => ScheduleStrategy::Weighted {
                w_d: config.w_d,
                w_f: config.w_f,
                w_s: config.w_s,
            },
            "fixed" => ScheduleStrategy::Fixed(config.n_min),
            _ => ScheduleStrategy::MaxPressure,
        }
    }
}

// ─── Compute Sub-Steps ────────────────────────────────────────────────────────

/// Compute n_k: number of sub-steps for the current macro-step (Definition σ)
///
/// If scheduler is disabled, always returns 1 (backward-compatible flat ticks).
pub fn compute_substeps(d: f64, f_friction: f64, s_shock: f64, config: &SchedulerConfig) -> u32 {
    if !config.enabled {
        return 1;
    }

    let n_min = config.n_min.max(1);
    let n_max = config.n_max.max(n_min);
    let range = (n_max - n_min) as f64;

    let pressure = match ScheduleStrategy::from_config(config) {
        ScheduleStrategy::MaxPressure => {
            d.max(f_friction).max(s_shock).clamp(0.0, 1.0)
        }
        ScheduleStrategy::Weighted { w_d, w_f, w_s } => {
            let total_w = w_d + w_f + w_s;
            if total_w <= 0.0 {
                0.0
            } else {
                ((w_d * d + w_f * f_friction + w_s * s_shock) / total_w).clamp(0.0, 1.0)
            }
        }
        ScheduleStrategy::Fixed(n) => return n.clamp(n_min, n_max),
    };

    let n_k = (n_min as f64 + range * pressure).floor() as u32;
    n_k.clamp(n_min, n_max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use pse_types::SchedulerConfig;

    fn config_disabled() -> SchedulerConfig {
        SchedulerConfig { enabled: false, n_min: 1, n_max: 10, ..SchedulerConfig::default() }
    }

    fn config_max_pressure() -> SchedulerConfig {
        SchedulerConfig {
            enabled: true,
            n_min: 1,
            n_max: 10,
            strategy: "max_pressure".to_string(),
            ..SchedulerConfig::default()
        }
    }

    // AT-S1: Disabled passthrough — n_k = 1 for all k
    #[test]
    fn at_s1_disabled_passthrough() {
        let cfg = config_disabled();
        assert_eq!(compute_substeps(0.0, 0.0, 0.0, &cfg), 1);
        assert_eq!(compute_substeps(0.9, 0.9, 0.9, &cfg), 1);
        assert_eq!(compute_substeps(1.0, 1.0, 1.0, &cfg), 1);
    }

    // AT-S2: Adaptive scaling
    #[test]
    fn at_s2_adaptive_scaling() {
        let cfg = config_max_pressure();
        // High deformation -> n_k > 1
        assert!(compute_substeps(1.0, 0.0, 0.0, &cfg) > 1);
        // Low deformation -> n_k = n_min
        assert_eq!(compute_substeps(0.0, 0.0, 0.0, &cfg), 1);
    }

    // AT-S3: Determinism — same inputs produce same n_k
    #[test]
    fn at_s3_determinism() {
        let cfg = config_max_pressure();
        let n1 = compute_substeps(0.5, 0.3, 0.7, &cfg);
        let n2 = compute_substeps(0.5, 0.3, 0.7, &cfg);
        assert_eq!(n1, n2);
    }

    // AT-S4: n_k is within [n_min, n_max]
    #[test]
    fn at_s4_bounds() {
        let cfg = config_max_pressure();
        for (d, f, s) in [(0.0, 0.0, 0.0), (1.0, 1.0, 1.0), (0.5, 0.8, 0.2)] {
            let n = compute_substeps(d, f, s, &cfg);
            assert!(n >= cfg.n_min && n <= cfg.n_max, "n={} out of [{}, {}]", n, cfg.n_min, cfg.n_max);
        }
    }

    // AT-S5: Backward compatibility — disabled gives same result as flat ticks
    #[test]
    fn at_s5_backward_compatibility() {
        let cfg = config_disabled();
        // Any input → 1 sub-step (matches pre-extension flat tick behavior)
        assert_eq!(compute_substeps(0.5, 0.5, 0.5, &cfg), 1);
    }
}
