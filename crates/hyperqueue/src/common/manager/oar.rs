use std::process::Command;
use std::str;
use std::time::Duration;

use serde_json::Value;

use crate::common::manager::common::format_duration;

fn parse_oar_job_remaining_time(job_id: &str, data: &str) -> anyhow::Result<Duration> {
    let data_json: Value = serde_json::from_str(data)?;

    let walltime = Duration::new(
        data_json[job_id]["walltime"]
            .to_string()
            .parse()
            //.unwrap_or_else(|| anyhow::anyhow!("Could not find walltime key for job {}", job_id))?;
            .unwrap(),
        0,
    );

    let start_time = Duration::new(
        data_json[job_id]["start_time"].to_string().parse().unwrap(),
        0,
    );

    let result = Command::new("date").args(["+%s"]).output()?;
    if !result.status.success() {
        anyhow::bail!(
            "date command exited with {}: {}\n{}",
            result.status,
            String::from_utf8_lossy(&result.stderr),
            String::from_utf8_lossy(&result.stdout)
        );
    }
    let output = String::from_utf8_lossy(&result.stdout).into_owned();
    log::debug!("date output: {}", output.trim());
    let current_time = Duration::new(output.parse().unwrap(), 0);

    // TODO: handle hold time
    let used = current_time - start_time;

    if walltime < used {
        anyhow::bail!("Oar: Used time is bigger then walltime");
    }

    Ok(walltime - used)
}

/// Calculates how much time is left for the given job using `qstat`.
pub fn get_remaining_timelimit(job_id: &str) -> anyhow::Result<Duration> {
    let result = Command::new("oarstat")
        .args(["-f", "-j", job_id, "-J"])
        .output()?;
    if !result.status.success() {
        anyhow::bail!(
            "oarstat command exited with {}: {}\n{}",
            result.status,
            String::from_utf8_lossy(&result.stderr),
            String::from_utf8_lossy(&result.stdout)
        );
    }

    let output = String::from_utf8_lossy(&result.stdout).into_owned();
    log::debug!("oarstat output: {}", output.trim());

    parse_oar_job_remaining_time(job_id, output.as_str())
}

/// Format a duration as a OAR time string, e.g. 01:05:02
pub fn format_oar_duration(duration: &Duration) -> String {
    format_duration(duration)
}

pub fn parse_oar_datetime(datetime: &str) -> anyhow::Result<chrono::NaiveDateTime> {
    Ok(chrono::NaiveDateTime::parse_from_str(
        datetime,
        "%Y-%m-%d_%H:%M:%S",
    )?)
}

#[cfg(test)]
mod test {
    use crate::common::manager::oar::parse_oar_datetime;

    #[test]
    fn test_parse_oar_datetime() {
        let date = parse_oar_datetime("2021-08-19_13:05:17").unwrap();
        assert_eq!(
            date.format("%d.%m.%Y %H:%M:%S").to_string(),
            "19.08.2021 13:05:17"
        );
    }
}
