//! Build-only stage profiling. Release builds without this feature have no timers.
#[cfg(feature = "profile")]
thread_local! {
    static STAGES: std::cell::RefCell<std::collections::BTreeMap<&'static str,(u64,u128)>> = Default::default();
}
#[cfg(feature = "profile")]
pub fn record(name: &'static str, elapsed: std::time::Duration) {
    STAGES.with(|stages| {
        let mut stages = stages.borrow_mut();
        let entry = stages.entry(name).or_default();
        entry.0 += 1; entry.1 += elapsed.as_nanos();
    });
}
#[cfg(feature = "profile")]
pub fn report() {
    STAGES.with(|stages| {
        let report: std::collections::BTreeMap<_,_> = stages.borrow().iter().map(|(name,(calls,ns))|
            (*name,serde_json::json!({"calls":calls,"seconds":*ns as f64 / 1e9}))).collect();
        eprintln!("{}",serde_json::json!({"rust_stage_profile":report}));
    });
}
macro_rules! timed {
    ($name:literal, $expression:expr) => {{
        #[cfg(feature = "profile")]
        {
            let start = std::time::Instant::now();
            let value = $expression;
            crate::profiling::record($name,start.elapsed());
            value
        }
        #[cfg(not(feature = "profile"))]
        { $expression }
    }};
}
