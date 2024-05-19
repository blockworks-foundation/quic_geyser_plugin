# QUIC GEYSER PLUGIN

Solana geyser plugin which implements quic frontend for faster http-3 access of solana data.

### Running Geyser

Compile and start the geyser plugin on a validator, and example config file is provided in `config.json`.

```
// To start a validator with geyser plugin add following argument to the solana validator
--geyser-plugin-config config.json
```

### Client

Client can be configured like this :

```
use quic_geyser_client::client::Client;

let url = "127.0.0.1:10800"; // Address of quic plugin on the RPC
let client = Client::new( url, &Keypair::new(), ConnectionParameters::default())
        .await
        .unwrap();

// to subscribe updates for stake program, slots and blockmeta
client.subscribe(vec![
            Filter::Account(AccountFilter {
                owner: Some("Stake11111111111111111111111111111111111111"),
                accounts: None,
            }),
            Filter::Slot,
            Filter::BlockMeta,
        ])
        .await
        .unwrap();
```

You can also subscibe to all the account updates by using filter `Filter::AccountsAll`
Similarly you can also subscibe to all transaction update by setting filter : `Filter::TransactionsAll,`.


### Tester

Tester is an example program, which gets all the possible updates from quic server and tests the bandwidth used and lags wrt rest of the cluster.

You can run tester with following command.

```
cargo run --bin geyser-quic-plugin-tester --release -- -u ip_address:10800 -r rpc_address
```