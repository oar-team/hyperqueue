use crate::common::utils::fs::get_current_dir;
use anyhow::Context;
use std::fmt::Write;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;
use tako::Map;

use crate::common::manager::info::ManagerType;
use crate::common::manager::oar::{format_oar_duration, parse_oar_datetime};
use crate::common::utils::time::local_to_system_time;
use crate::server::autoalloc::queue::common::{
    build_worker_args, check_command_output, create_allocation_dir, create_command, submit_script,
    wrap_worker_cmd, ExternalHandler,
};
use crate::server::autoalloc::queue::{
    AllocationExternalStatus, AllocationStatusMap, AllocationSubmissionResult, QueueHandler,
    SubmitMode,
};
use crate::server::autoalloc::{Allocation, AllocationId, AutoAllocResult, QueueId, QueueInfo};

pub struct OarHandler {
    handler: ExternalHandler,
}

impl OarHandler {
    pub fn new(server_directory: PathBuf, name: Option<String>) -> anyhow::Result<Self> {
        let handler = ExternalHandler::new(server_directory, name)?;
        Ok(Self { handler })
    }
}

impl QueueHandler for OarHandler {
    fn submit_allocation(
        &mut self,
        queue_id: QueueId,
        queue_info: &QueueInfo,
        worker_count: u64,
        mode: SubmitMode,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationSubmissionResult>>>> {
        let queue_info = queue_info.clone();
        let timelimit = queue_info.timelimit;
        let hq_path = self.handler.hq_path.clone();
        let server_directory = self.handler.server_directory.clone();
        let name = self.handler.name.clone();
        let allocation_num = self.handler.create_allocation_id();

        Box::pin(async move {
            let directory = create_allocation_dir(
                server_directory.clone(),
                queue_id,
                name.as_ref(),
                allocation_num,
            )?;
            let worker_args =
                build_worker_args(&hq_path, ManagerType::Oar, &server_directory, &queue_info);
            let worker_args = wrap_worker_cmd(
                worker_args,
                queue_info.worker_start_cmd.as_deref(),
                queue_info.worker_stop_cmd.as_deref(),
            );

            let script = build_oar_submit_script(
                worker_count,
                timelimit,
                &format!("hq-alloc-{queue_id}"),
                &directory.join("stdout").display().to_string(),
                &directory.join("stderr").display().to_string(),
                &queue_info.additional_args.join(" "),
                &worker_args,
                mode,
            );
            let job_id = submit_script(script, "oarsub", &directory, |output| {
                parse_oarsub_output(output)
            })
            .await;

            Ok(AllocationSubmissionResult {
                id: job_id,
                working_dir: directory,
            })
        })
    }

    fn get_status_of_allocations(
        &self,
        allocations: &[&Allocation],
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<AllocationStatusMap>>>> {
        let mut arguments = vec!["oarstat"];
        for &allocation in allocations {
            arguments.extend_from_slice(&["-j", &allocation.id]);
        }

        // -x will also display finished jobs
        arguments.extend_from_slice(&["-f", "-J"]);

        let allocation_ids: Vec<AllocationId> =
            allocations.iter().map(|alloc| alloc.id.clone()).collect();
        let workdir = allocations
            .first()
            .map(|alloc| alloc.working_dir.clone())
            .unwrap_or_else(get_current_dir);

        log::debug!("Running OAR command `{}`", arguments.join(" "));

        let mut command = create_command(arguments, &workdir);

        Box::pin(async move {
            let output = command.output().await.context("oarstat start failed")?;
            let output = check_command_output(output).context("oarstat execution failed")?;

            log::trace!(
                "OAR oarstat output\nStdout\n{}Stderr\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );

            let data: serde_json::Value = serde_json::from_slice(&output.stdout)
                .context("Cannot parse oarstat JSON output")?;

            let mut result = Map::with_capacity(allocation_ids.len());

            let jobs = &data;
            for allocation_id in allocation_ids {
                let allocation = &jobs[&allocation_id];
                if !allocation.is_null() {
                    let status = parse_allocation_status(allocation);
                    result.insert(allocation_id, status);
                }
            }

            Ok(result)
        })
    }

    fn remove_allocation(
        &self,
        allocation: &Allocation,
    ) -> Pin<Box<dyn Future<Output = AutoAllocResult<()>>>> {
        let allocation_id = allocation.id.clone();
        let workdir = allocation.working_dir.clone();

        Box::pin(async move {
            let arguments = vec!["oardel", &allocation_id];
            log::debug!("Running OAR command `{}`", arguments.join(" "));

            let mut command = create_command(arguments, &workdir);
            let output = command.output().await?;
            check_command_output(output)?;
            Ok(())
        })
    }
}

fn parse_oarsub_output(output: &str) -> AutoAllocResult<AllocationId> {
    let mut iter = output
        .split("\n")
        .filter(|&l| l.to_string().starts_with("OAR_JOB_ID="));
    let job_id = iter
        .next()
        .expect("oarsub failed")
        .strip_prefix("OAR_JOB_ID=")
        .expect("oarsub failed")
        .to_string();
    Ok(job_id)
}

fn parse_allocation_status(
    allocation: &serde_json::Value,
) -> AutoAllocResult<AllocationExternalStatus> {
    let state = get_json_str(&allocation["state"], "Job state")?;
    let start_time_key = "start_time";
    let stop_time_key = "stop_time";

    let parse_time = |key: &str| {
        let value = &allocation[key];
        value
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing time key {} in OAR", key))
            .and_then(|v| Ok(local_to_system_time(parse_oar_datetime(v)?)))
    };

    let status = match state {
        //OAR jos states: ‘Waiting’, ‘Hold’, ‘toLaunch’, ‘toError’, ‘toAckReservation’, ‘Launching’, ‘Finishing’, ‘Running’, ‘Suspended’, ‘Resuming’, ‘Terminated’, ‘Error’
        "Waiting" | "toLaunch" | "Launching" => AllocationExternalStatus::Queued,
        "Running" | "Finishing" => AllocationExternalStatus::Running,
        "Terminated" | "Error" => {
            let exit_status = get_json_number(&allocation["exit_code"], "Exit status").ok();
            let started_at = parse_time(start_time_key).ok();
            let finished_at = parse_time(stop_time_key)?;

            if exit_status == Some(0) {
                AllocationExternalStatus::Finished {
                    started_at,
                    finished_at,
                }
            } else {
                AllocationExternalStatus::Failed {
                    started_at,
                    finished_at,
                }
            }
        }
        status => anyhow::bail!("Unknown OAR job status {}", status),
    };
    Ok(status)
}

#[allow(clippy::too_many_arguments)]
fn build_oar_submit_script(
    nodes: u64,
    timelimit: Duration,
    name: &str,
    stdout: &str,
    stderr: &str,
    oarsub_args: &str,
    worker_cmd: &str,
    mode: SubmitMode,
) -> String {
    let mut script = format!(
        r##"#!/bin/bash
#OAR -l nodes={nodes},walltime={walltime}
#OAR -N {name}
#OAR -o {stdout}
#OAR -e {stderr}
"##,
        nodes = nodes,
        name = name,
        stdout = stdout,
        stderr = stderr,
        walltime = format_oar_duration(&timelimit)
    );

    if !oarsub_args.is_empty() {
        writeln!(script, "#OAR {oarsub_args}").unwrap();
    }
    match mode {
        SubmitMode::DryRun => script.push_str("exit 0\n"),
        SubmitMode::Submit => {}
    }

    script.push('\n');

    if nodes > 1 {
        write!(script, "oarsh '{worker_cmd}'").unwrap();
    } else {
        script.push_str(worker_cmd);
    };
    script
}

fn get_json_str<'a>(value: &'a serde_json::Value, context: &str) -> AutoAllocResult<&'a str> {
    value
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("JSON key {} not found", context))
}
fn get_json_number(value: &serde_json::Value, context: &str) -> AutoAllocResult<u64> {
    value
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("JSON key {} not found", context))
}
