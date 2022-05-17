use cubic_spline::{Points, SplineOpts};
use crate::error::{Error, Result};

pub fn calc_cubic_spline(total_points: usize, key_points: Vec<f64>) -> Result<Vec<f64>> {
    assert!(key_points.len() <= total_points);
    let segments = total_points as f64 / (key_points.len() - 1) as f64 + 2.;
    let source: Vec<(f64, f64)> = key_points
        .iter()
        .enumerate()
        .map(|(idx, p)| (segments * idx as f64, *p))
        .collect();
    let opts = SplineOpts::new()
        .tension(0.5)
        .num_of_segments(segments as u32);

    let result = Points::from(&source)
        .calc_spline(&opts).map_err(|e| Error::External(e.to_string()))?
        .into_inner()
        .iter()
        .map(|v| v.y)
        .collect();

    Ok(result)
}