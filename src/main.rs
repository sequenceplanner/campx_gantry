use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use anyhow::Result;
use clap::Parser;
use std::sync::{Arc, Mutex};
use std::collections::{HashSet,HashMap};
use std::time::Duration;
use tokio::sync::mpsc;

mod opc;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Opc server URI
    #[arg(long, env="OPC_URI", default_value_t = String::from("opc.tcp://192.168.1.33:4840"))]
    opc_uri: String,

    /// Opc server namespace
    #[arg(long, env="OPC_NAMESPACE", default_value_t = 4)]
    opc_ns: u16,
}

pub static ARGS: Lazy<ArcSwap<Args>> = Lazy::new(|| ArcSwap::from_pointee(Args::parse()));

#[tokio::main]
async fn main() -> Result<()> {
    let state = Arc::new(Mutex::new(HashMap::<u32, serde_json::Value>::new()));
    let (opc_tx, opc_rx) = mpsc::channel::<Vec<(u32, serde_json::Value)>>(32);
    let (sp_tx, sp_rx) = mpsc::channel(32);

    let inputs = vec![
        ("opc_start_flag", 45),
        ("opc_reference_position", 46),
        ("opc_reference_speed", 47),
        ("opc_done_flag", 94),
        ("opc_current_position", 306),
    ];

    let outputs = vec![
        ("opc_write_start_flag", 45),
        ("opc_write_reference_position", 46),
        ("opc_write_reference_speed", 47),
    ];

    let opc_ids = inputs.iter().chain(outputs.iter()).map(|(_, id)| *id).collect::<HashSet<u32>>();

    // start the sp state manager.
    tokio::task::spawn(async move {
        // uses REDIS_HOST and REDIS_PORT from env.
        match micro_sp::redis_state_manager(sp_rx, make_sp_state()).await {
            Ok(()) => (),
            Err(e) => eprintln!("error {}", e),
        }
    });

    // Print the state.
    let state_cb = state.clone();
    tokio::task::spawn( async move {
        let err: Result<()> =
        async {
            let mut prev_state = HashMap::new();
            let mut prev_opc_outputs = vec![];
            loop {
                let state = state_cb.lock().unwrap().clone();
                if state != prev_state {
                    // perform mapping from int to string
                    let opc_mapped = state.iter().flat_map(|(k, v)| {
                        inputs.iter().find(|(_,id)| id == k).map(|(key, _)| (key.to_string(), v.clone()))
                    }).collect::<HashMap<String, serde_json::Value>>();

                    println!("opc state");
                    for (k, v) in &opc_mapped {
                        println!("  {} - {}", k, v);
                    }
                    println!();

                    // write to sp
                    let sp_state = make_opc_input_state(opc_mapped);
                    sp_tx.send(micro_sp::StateManagement::SetPartialState(sp_state)).await?;
                }

                let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                sp_tx
                    .send(micro_sp::StateManagement::GetState(response_tx))
                    .await?;

                let sp_state = response_rx.await?;

                let opc_outputs = extract_opc_outputs(&sp_state);
                if opc_outputs != prev_opc_outputs {
                    println!("sp state");
                    for (k, v) in &opc_outputs {
                        println!("  {} - {}", k, v);
                    }
                    println!();

                    // map back to opc ids
                    let opc_outputs = opc_outputs.iter().flat_map(|(k, v)| {
                        outputs.iter().find(|(key,_)| key == k).map(|(_, id)| (*id, v.clone()))
                    }).collect::<Vec<_>>();
                    let _ = opc_tx.try_send(opc_outputs);
                }

                prev_state = state;
                prev_opc_outputs = opc_outputs;

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }.await;
        if let Err(e) = err {
            println!("Error: {}", e);
        }
    });

    let opc_ids = opc_ids.into_iter().collect::<Vec<_>>();
    opc::run(state, opc_rx, &opc_ids).await?;

    Ok(())
}



fn make_sp_state() -> micro_sp::State {
    use micro_sp::*;
    let state = State::new();

    let start_flag = bv!("opc_start_flag");
    let reference_position = fv!("opc_reference_position");
    let reference_speed = fv!("opc_reference_speed");
    let done_flag = bv!("opc_done_flag");
    let current_position = fv!("opc_current_position");

    let state = state.add(assign!(start_flag, false.to_spvalue()));
    let state = state.add(assign!(reference_position, 0.0.to_spvalue()));
    let state = state.add(assign!(reference_speed, 0.0.to_spvalue()));
    let state = state.add(assign!(done_flag, false.to_spvalue()));
    let state = state.add(assign!(current_position, 0.0.to_spvalue()));

    let write_start_flag = bv!("opc_write_start_flag");
    let write_reference_position = fv!("opc_write_reference_position");
    let write_reference_speed = fv!("opc_write_reference_speed");

    let state = state.add(assign!(write_start_flag, false.to_spvalue()));
    let state = state.add(assign!(write_reference_position, 0.0.to_spvalue()));
    let state = state.add(assign!(write_reference_speed, 0.0.to_spvalue()));

    state
}

fn extract_opc_outputs(state: &micro_sp::State) -> Vec<(String, serde_json::Value)> {
    let mut json_state = vec![];
    get_json("opc_write_start_flag", state).map(|(k,v)| json_state.push((k, v)));
    get_json("opc_write_reference_position", state).map(|(k,v)| json_state.push((k, v)));
    get_json("opc_write_reference_speed", state).map(|(k,v)| json_state.push((k, v)));

    json_state
}

fn make_opc_input_state(opc_state: HashMap<String, serde_json::Value>) -> micro_sp::State {
    use micro_sp::*;
    let state = State::new();

    let start_flag = bv!("opc_start_flag");
    let start_flag_v = opc_state.get("opc_start_flag").and_then(|v| v.as_bool()).unwrap_or(false);
    let state = state.add(assign!(start_flag, start_flag_v.to_spvalue()));

    let reference_position = fv!("opc_reference_position");
    let refpos_v = opc_state.get("opc_reference_position").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let state = state.add(assign!(reference_position, refpos_v.to_spvalue()));

    let reference_speed = fv!("opc_reference_speed");
    let refspeed_v = opc_state.get("opc_reference_speed").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let state = state.add(assign!(reference_speed, refspeed_v.to_spvalue()));

    let done_flag = bv!("opc_done_flag");
    let done_flag_v = opc_state.get("opc_done_flag").and_then(|v| v.as_bool()).unwrap_or(false);
    let state = state.add(assign!(done_flag, done_flag_v.to_spvalue()));

    let current_pos = fv!("opc_current_position");
    let current_pos_v = opc_state.get("opc_current_position").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let state = state.add(assign!(current_pos, current_pos_v.to_spvalue()));

    state
}


fn get_json(key: &str, state: &micro_sp::State) -> Option<(String, serde_json::Value)> {
    use micro_sp::*;
    state.state.get(key).map(|v| {
        let json = match &v.val {
            SPValue::Bool(BoolOrUnknown::Bool(b)) => serde_json::Value::Bool(*b),
            SPValue::Float64(FloatOrUnknown::Float64(f)) => {
                serde_json::Value::Number(
                    serde_json::Number::from_f64(f.into_inner()).expect("not proper f64"),
                )
            },
            SPValue::Int64(IntOrUnknown::Int64(i)) => serde_json::Value::Number((*i).into()),
            SPValue::String(StringOrUnknown::String(s)) => serde_json::Value::String(s.into()),
            _ => serde_json::Value::Null,
        };
        (key.to_string(), json)
    })
}
