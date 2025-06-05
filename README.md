gantry driver
===


```
martin@martin-mbp ~/volvo/campx/gantry (master) $ cargo run -- --help
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.12s
     Running `target/debug/gantry --help`
Usage: gantry [OPTIONS]

Options:
      --opc-uri <OPC_URI>  Opc server URI [env: OPC_URI=] [default: opc.tcp://192.168.1.33:4840]
      --opc-ns <OPC_NS>    Opc server namespace [env: OPC_NAMESPACE=] [default: 4]
  -h, --help               Print help
  -V, --version            Print version
```

example:

```
martin@martin-mbp ~/volvo/campx/gantry $ REDIS_HOST=192.168.1.15 cargo run
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.13s
     Running `target/debug/gantry`
sp state
  opc_write_start_flag - false
  opc_write_reference_position - 0.0
  opc_write_reference_speed - 0.0

to write: 45 - false
WARNING: cannot write to 45 we havent read the item yet
to write: 46 - 0.0
WARNING: cannot write to 46 we havent read the item yet
to write: 47 - 0.0
WARNING: cannot write to 47 we havent read the item yet
opc state
  opc_done_flag - false
  opc_current_position - 500.0
  opc_start_flag - false
  opc_reference_speed - 100.0
  opc_reference_position - 500.0
```
