//! ROS 2 action server for the gantry.
//!
//! Offers a `gantry_interfaces/action/Move` action and runs each goal through
//! the same Redis keys the `gantry` driver mirrors to the PLC: it writes the
//! `opc_write_*` setpoints, raises the start flag, waits for the PLC's done
//! flag, then drops the start flag again. The `gantry` driver has to be
//! running for anything to reach the PLC.
//!
//! It also publishes the PLC's read values as `gantry_interfaces/msg/State`.

use anyhow::Result;
use clap::Parser;
use futures::{Stream, StreamExt};
use micro_sp::*;
use r2r::gantry_interfaces::action::Move;
use r2r::gantry_interfaces::msg::State as GantryState;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::Instant;

const LOG_TARGET: &str = "campx_gantry_ros";

/// How long the PLC gets to echo a write back before the goal is aborted.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_PERIOD: Duration = Duration::from_millis(100);

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Name of the action server
    #[arg(long, env = "GANTRY_ACTION_NAME", default_value_t = String::from("/gantry/move"))]
    action_name: String,

    /// Seconds a move may take before the goal is aborted
    #[arg(long, env = "GANTRY_GOAL_TIMEOUT", default_value_t = 120)]
    goal_timeout: u64,

    /// Topic the PLC's read values are published on
    #[arg(long, env = "GANTRY_STATE_TOPIC", default_value_t = String::from("/gantry/state"))]
    state_topic: String,
}

/// What the PLC currently reports, as mirrored into Redis by the driver.
#[derive(Debug, Clone, Copy)]
struct Readback {
    start_flag: bool,
    reference_position: f64,
    reference_speed: f64,
    done_flag: bool,
    current_position: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    initialize_env_logger();
    let args = Args::parse();

    let connection_manager = ConnectionManager::new().await;
    let con = connection_manager.get_connection().await;

    let ctx = r2r::Context::create()?;
    let mut node = r2r::Node::create(ctx, "campx_gantry", "")?;
    let requests = node.create_action_server::<Move::Action>(&args.action_name)?;
    log::info!(target: LOG_TARGET, "Action server up on {}.", args.action_name);
    let publisher =
        node.create_publisher::<GantryState>(&args.state_topic, r2r::QosProfile::default())?;
    log::info!(target: LOG_TARGET, "Publishing state on {}.", args.state_topic);

    // spin_once blocks, so the node gets a thread of its own.
    std::thread::spawn(move || {
        loop {
            node.spin_once(Duration::from_millis(100));
        }
    });

    tokio::task::spawn(publish_state(publisher, con.clone()));
    serve(requests, con, Duration::from_secs(args.goal_timeout)).await;
    Ok(())
}

/// Publish the read values every [`POLL_PERIOD`]. Nothing goes out while any
/// of them is missing from Redis, e.g. before the driver has connected.
async fn publish_state(publisher: r2r::Publisher<GantryState>, mut con: SPConnection) {
    loop {
        if let Some(r) = read(&mut con).await {
            let msg = GantryState {
                start_flag: r.start_flag,
                reference_position: r.reference_position,
                reference_speed: r.reference_speed,
                done_flag: r.done_flag,
                current_position: r.current_position,
            };
            if let Err(e) = publisher.publish(&msg) {
                log::error!(target: LOG_TARGET, "Could not publish state: {}", e);
            }
        }
        tokio::time::sleep(POLL_PERIOD).await;
    }
}

async fn serve(
    mut requests: impl Stream<Item = r2r::ActionServerGoalRequest<Move::Action>> + Unpin,
    con: SPConnection,
    goal_timeout: Duration,
) {
    // The gantry can only do one move at a time.
    let busy = Arc::new(AtomicBool::new(false));

    while let Some(req) = requests.next().await {
        if busy.swap(true, Ordering::SeqCst) {
            log::warn!(target: LOG_TARGET, "Rejecting goal {}: a move is already running.", req.uuid);
            let _ = req.reject();
            continue;
        }

        let (goal, cancel) = match req.accept() {
            Ok(accepted) => accepted,
            Err(e) => {
                log::error!(target: LOG_TARGET, "Could not accept goal: {}", e);
                busy.store(false, Ordering::SeqCst);
                continue;
            }
        };

        let busy = busy.clone();
        let con = con.clone();
        tokio::task::spawn(async move {
            run_goal(con, goal, cancel, goal_timeout).await;
            busy.store(false, Ordering::SeqCst);
        });
    }
}

async fn run_goal(
    mut con: SPConnection,
    mut goal: r2r::ActionServerGoal<Move::Action>,
    mut cancel: impl Stream<Item = r2r::ActionServerCancelRequest> + Unpin,
    goal_timeout: Duration,
) {
    let Move::Goal { position, speed } = goal.goal.clone();
    log::info!(target: LOG_TARGET, "Goal {}: move to {} at {}.", goal.uuid, position, speed);

    let mut con_move = con.clone();
    let outcome = tokio::select! {
        result = move_gantry(&mut con_move, goal.clone(), position, speed, goal_timeout) => Some(result),
        Some(req) = cancel.next() => {
            req.accept();
            None
        }
    };

    // Whatever happened, drop the start flag so the next goal gives the PLC a
    // fresh rising edge.
    StateManager::set_sp_value(&mut con, "opc_write_start_flag", &false.to_spvalue()).await;

    let final_position = read(&mut con).await.map(|r| r.current_position).unwrap_or(0.0);
    let result = match &outcome {
        Some(Ok(())) => Move::Result {
            success: true,
            message: String::new(),
            position: final_position,
        },
        Some(Err(message)) => Move::Result {
            success: false,
            message: message.clone(),
            position: final_position,
        },
        None => Move::Result {
            success: false,
            message: "canceled".to_string(),
            position: final_position,
        },
    };

    let sent = match outcome {
        Some(Ok(())) => {
            log::info!(target: LOG_TARGET, "Goal {} succeeded at {}.", goal.uuid, final_position);
            goal.succeed(result)
        }
        Some(Err(message)) => {
            log::error!(target: LOG_TARGET, "Goal {} aborted: {}", goal.uuid, message);
            goal.abort(result)
        }
        None => {
            log::warn!(target: LOG_TARGET, "Goal {} canceled.", goal.uuid);
            goal.cancel(result)
        }
    };
    if let Err(e) = sent {
        log::error!(target: LOG_TARGET, "Could not send result of goal {}: {}", goal.uuid, e);
    }
}

/// The start/done handshake with the PLC: start low, setpoints, start high,
/// wait for done.
async fn move_gantry(
    con: &mut SPConnection,
    goal: r2r::ActionServerGoal<Move::Action>,
    position: f64,
    speed: f64,
    goal_timeout: Duration,
) -> Result<(), String> {
    // Each step waits for the PLC to echo it back before the next one goes out.
    // 1. Start low, so the PLC gets a fresh rising edge for this move.
    StateManager::set_sp_value(con, "opc_write_start_flag", &false.to_spvalue()).await;
    wait_for(con, "the start flag going low", |r| !r.start_flag).await?;

    // 2. The setpoints, while start is low.
    let setpoints = State::new()
        .add(assign!(fv!("opc_write_reference_position"), position.to_spvalue()), LOG_TARGET)
        .add(assign!(fv!("opc_write_reference_speed"), speed.to_spvalue()), LOG_TARGET);
    StateManager::set_state(con, &setpoints).await;
    wait_for(con, "the setpoints", |r| {
        approx_eq(r.reference_position, position) && approx_eq(r.reference_speed, speed)
    })
    .await?;

    // 3. Start high.
    StateManager::set_sp_value(con, "opc_write_start_flag", &true.to_spvalue()).await;
    let started = wait_for(con, "the start flag", |r| r.start_flag).await?;

    // A done flag still up from the previous move doesn't count: it has to
    // drop before a rising one means this move is finished.
    let mut stale_done = started.done_flag;
    let deadline = Instant::now() + goal_timeout;
    loop {
        if let Some(r) = read(con).await {
            let _ = goal.publish_feedback(Move::Feedback {
                current_position: r.current_position,
            });
            if !r.done_flag {
                stale_done = false;
            } else if !stale_done {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("the PLC did not report done within {}s", goal_timeout.as_secs()));
        }
        tokio::time::sleep(POLL_PERIOD).await;
    }
}

/// Poll Redis until `cond` holds, or fail after [`HANDSHAKE_TIMEOUT`].
async fn wait_for(
    con: &mut SPConnection,
    what: &str,
    cond: impl Fn(&Readback) -> bool,
) -> Result<Readback, String> {
    let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    let mut last = None;
    loop {
        if let Some(r) = read(con).await {
            if cond(&r) {
                return Ok(r);
            }
            last = Some(r);
        }
        if Instant::now() >= deadline {
            let seen = match last {
                Some(r) => format!(
                    "it reads start_flag={}, reference_position={}, reference_speed={}",
                    r.start_flag, r.reference_position, r.reference_speed
                ),
                None => "no read values in Redis; is the gantry driver running and connected?"
                    .to_string(),
            };
            return Err(format!(
                "the PLC did not acknowledge {} within {}s; {}",
                what,
                HANDSHAKE_TIMEOUT.as_secs(),
                seen
            ));
        }
        tokio::time::sleep(POLL_PERIOD).await;
    }
}

async fn read(con: &mut SPConnection) -> Option<Readback> {
    let keys = vec![
        "opc_start_flag".to_string(),
        "opc_reference_position".to_string(),
        "opc_reference_speed".to_string(),
        "opc_done_flag".to_string(),
        "opc_current_position".to_string(),
    ];
    let state = StateManager::get_state_for_keys(con, &keys, LOG_TARGET).await?;
    let value = |key: &str| state.state.get(key).map(|v| v.val.clone());
    let as_bool = |key: &str| match value(key) {
        Some(SPValue::Bool(BoolOrUnknown::Bool(b))) => Some(b),
        _ => None,
    };
    let as_f64 = |key: &str| match value(key) {
        Some(SPValue::Float64(FloatOrUnknown::Float64(f))) => Some(f.into_inner()),
        _ => None,
    };

    Some(Readback {
        start_flag: as_bool("opc_start_flag")?,
        reference_position: as_f64("opc_reference_position")?,
        reference_speed: as_f64("opc_reference_speed")?,
        done_flag: as_bool("opc_done_flag")?,
        current_position: as_f64("opc_current_position")?,
    })
}

/// The PLC may store setpoints as 32-bit floats, so the echo is not exact.
fn approx_eq(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-4 * b.abs().max(1.0)
}
