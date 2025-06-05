use opcua::{
    client::{ClientBuilder, DataChangeCallback, IdentityToken, Session},
    crypto::SecurityPolicy,
    types::{
        DataValue, MessageSecurityMode, MonitoredItemCreateRequest, NodeId, StatusCode,
        TimestampsToReturn, UserTokenPolicy, Variant, WriteValue
    },
};
use anyhow::Result;
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use tokio::sync::mpsc::Receiver;

pub async fn run(state: Arc<Mutex<HashMap<u32, serde_json::Value>>>,
                 mut input: Receiver<Vec<(u32, serde_json::Value)>>,
                 node_ids: &[u32]) -> Result<()> {
    let mut client = ClientBuilder::new()
        .application_name("Simple Client")
        .application_uri("urn:SimpleClient")
        .product_uri("urn:SimpleClient")
        .trust_server_certs(true)
        .create_sample_keypair(true)
        .session_retry_limit(3)
        .client()
        .unwrap();

    let (session, event_loop) = client
        .connect_to_matching_endpoint(
            (
                crate::ARGS.load().opc_uri.as_ref(),
                SecurityPolicy::None.to_str(),
                MessageSecurityMode::None,
                UserTokenPolicy::anonymous(),
            ),
            IdentityToken::Anonymous,
        )
        .await?;

    let internal_state = Arc::new(Mutex::new(HashMap::new()));

    let handle = event_loop.spawn();
    session.wait_for_connection().await;

    if let Err(result) = subscribe_to_variables(
        session.clone(),
        internal_state.clone(),
        state.clone(),
        node_ids,
    ).await {
        println!(
            "ERROR: Got an error while subscribing to variables - {}",
            result
        );
        let _ = session.disconnect().await;
    }

    // It's a good idea to intercept ctrl-c and gracefully shut down the client
    // since servers will keep sessions alive for some time after a sudden disconnect.
    // This way, the session will be properly closed.
    let session_c = session.clone();
    tokio::task::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            eprintln!("Failed to register CTRL-C handler: {e}");
            return;
        }
        let _ = session_c.disconnect().await;
    });

    // State writer
    let internal_state_cb = internal_state.clone();
    tokio::task::spawn(async move {
        loop {
            if let Some(write_vec) = input.recv().await {
                write_opc(session.clone(), internal_state_cb.clone(), &write_vec).await;
            }
        }
    });

    handle.await.unwrap();

    Ok(())
}

async fn subscribe_to_variables(session: Arc<Session>, internal_state: Arc<Mutex<HashMap<u32, Variant>>>, public_state: Arc<Mutex<HashMap<u32, serde_json::Value>>>, ids: &[u32]) -> Result<(), StatusCode> {
    let ns = crate::ARGS.load().opc_ns;

    let state_cb = internal_state.clone();

    // Creates a subscription with a data change callback
    let subscription_id = session
        .create_subscription(
            Duration::ZERO,
            10,
            30,
            0,
            0,
            true,
            DataChangeCallback::new(move |dv, item| {
                let mut map = state_cb.lock().unwrap();
                let node_id = item.item_to_monitor().node_id.as_u32().expect("malformed key");
                if let Some(value) = dv.value {
                    let json = opc_variant_to_serde_value(&value);
                    let key = node_id.clone();
                    match map.entry(node_id) {
                        Entry::Occupied(mut entry) => { *entry.get_mut() = value; },
                        Entry::Vacant(entry) => { entry.insert(value); }
                    }

                    // Update the public state.
                    public_state.lock().unwrap().insert(key, json);
                }
            }),
        )
        .await?;
    // println!("Created a subscription with id = {}", subscription_id);

    // Create some monitored items
    //     &[MessageVariable::new(&act_pos, "ns=4;i=46"),
    //       MessageVariable::new(&act_speed, "ns=4;i=47"),
    //       MessageVariable::new(&act_start, "ns=4;i=45"),
    //       MessageVariable::new(&done, "ns=4;i=94"),
    //       MessageVariable::new(&real_pos, "ns=4;i=306"),
    let items_to_create: Vec<MonitoredItemCreateRequest> = ids
        .iter()
        .map(|v| NodeId::new(ns, *v).into())
        .collect();
    let _ = session
        .create_monitored_items(subscription_id, TimestampsToReturn::Both, items_to_create)
        .await?;

    Ok(())
}

fn opc_variant_to_serde_value(variant: &Variant) -> serde_json::Value {
    match variant {
        Variant::Empty => serde_json::Value::Null,
        Variant::Boolean(b) => serde_json::Value::Bool(*b),
        Variant::SByte(i8) => serde_json::Value::Number((*i8).into()),
        Variant::Byte(u8) => serde_json::Value::Number((*u8).into()),
        Variant::Int16(i16) => serde_json::Value::Number((*i16).into()),
        Variant::UInt16(u16) => serde_json::Value::Number((*u16).into()),
        Variant::Int32(i32) => serde_json::Value::Number((*i32).into()),
        Variant::UInt32(u32) => serde_json::Value::Number((*u32).into()),
        Variant::Int64(i64) => serde_json::Value::Number((*i64).into()),
        Variant::UInt64(u64) => serde_json::Value::Number((*u64).into()),
        Variant::Float(f32) => serde_json::Value::Number(
            serde_json::Number::from_f64(*f32 as f64).expect("not proper f64"),
        ),
        Variant::Double(f64) => {
            serde_json::Value::Number(serde_json::Number::from_f64(*f64).expect("not proper f64"))
        }
        Variant::String(s) => match s.value() {
            Some(s) => serde_json::Value::String(s.into()),
            None => serde_json::Value::Null,
        },
        _ => panic!("Not implemented"),
    }
}

fn mutate_variant_from_json(variant: &mut Variant, new_value: &serde_json::Value) {
    match variant {
        Variant::Empty => {},
        Variant::Boolean(b) => {
            if !new_value.is_boolean() {
                println!("warning: not a bool");
                return;
            }
            *b = new_value.as_bool().unwrap();
        },
        Variant::SByte(i8) => {
            if !new_value.is_number() {
                println!("warning: not a number");
                return;
            }
            *i8 = new_value.as_i64().unwrap() as i8;
        },
        Variant::Byte(u8) => {
            if !new_value.is_number() {
                println!("warning: not a number");
                return;
            }
            *u8 = new_value.as_i64().unwrap() as u8;
        },
        Variant::Int16(i16) => {
            if !new_value.is_number() {
                println!("warning: not a number");
                return;
            }
            *i16 = new_value.as_i64().unwrap() as i16;
        },
        Variant::UInt16(u16) => {
            if !new_value.is_number() {
                println!("warning: not a number");
                return;
            }
            *u16 = new_value.as_i64().unwrap() as u16;
        },
        Variant::Int32(i32) => {
            if !new_value.is_number() {
                println!("warning: not a number");
                return;
            }
            *i32 = new_value.as_i64().unwrap() as i32;
        }
        Variant::UInt32(u32) => {
            if !new_value.is_number() {
                println!("warning: not a number");
                return;
            }
            *u32 = new_value.as_i64().unwrap() as u32;
        },
        Variant::Int64(i64) => {
            if !new_value.is_number() {
                println!("warning: not a number");
                return;
            }
            *i64 = new_value.as_i64().unwrap();
        },
        Variant::UInt64(u64) => {
            if !new_value.is_number() {
                println!("warning: not a number");
                return;
            }
            *u64 = new_value.as_i64().unwrap() as u64;
        },
        Variant::Float(f32) => {
            if !new_value.is_number() {
                println!("warning: not a number");
                return;
            }
            *f32 = new_value.as_f64().unwrap() as f32;
        },
        Variant::Double(f64) => {
            if !new_value.is_number() {
                println!("warning: not a number");
                return;
            }
            *f64 = new_value.as_f64().unwrap();
        },
        Variant::String(s) => {
            if !new_value.is_string() {
                println!("warning: not a number");
                return;
            }
            *s = new_value.as_str().unwrap().into();
        }
        _ => panic!("Not implemented"),
    }
}


async fn write_opc(session: Arc<Session>, state: Arc<Mutex<HashMap<u32, Variant>>>,
                   to_write: &[(u32, serde_json::Value)]) {
    let items_to_write = to_write.iter().flat_map(|(key, json_value)| {
        println!("to write: {} - {}", key, json_value);

        // we need to lookup the correct datatype from the current state.
        let state = state.lock().unwrap();
        match state.get(key) {
            Some(v) => {
                let mut new_variant = v.clone();
                mutate_variant_from_json(&mut new_variant, json_value);

                let wv = WriteValue {
                    node_id: NodeId::new(crate::ARGS.load().opc_ns, *key),
                    attribute_id: 13, // Value attribute id
                    index_range: opcua::types::string::UAString::default(),
                    value: DataValue::value_only(new_variant),
                };

                Some(wv)
            },
            None => {
                println!("WARNING: cannot write to {} we havent read the item yet", key);
                None
            }
        }
    }).collect::<Vec<WriteValue>>();

    if items_to_write.is_empty() {
        return;
    }

    let result = session.write(&items_to_write).await;
    match result {
        Ok(r) => {
            r.iter().for_each(|r| {
                if !r.is_good() {
                    println!("WARNING: write result: {}", r);
                }
            });
        }
        Err(e) => {
            println!("WARNING: write not successful {}", e);
        }
    }
}
