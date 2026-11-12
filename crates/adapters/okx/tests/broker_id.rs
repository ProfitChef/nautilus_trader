// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Fork-only: `OKX_BROKER_ID` replaces the NautilusTrader broker code in the order `tag`.
//!
//! The broker code is read once per process, so this file is its own test binary with a single
//! test: the variable is set before the first order, whichever runner (nextest or `cargo test`)
//! executes it. The default code without the variable is asserted by the integration suite.

use std::{path::PathBuf, sync::Arc};

use axum::{Router, http::HeaderMap, response::Json, routing::post};
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    identifiers::{ClientOrderId, InstrumentId},
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use nautilus_okx::{
    common::{
        consts::{OKX_BROKER_ID_ENV, OKX_NAUTILUS_BROKER_ID, okx_broker_id},
        enums::{OKXEnvironment, OKXTradeMode},
        models::OKXInstrument,
        parse::parse_instrument_any,
    },
    http::client::{OKXHttpClient, OKXResponse},
};
use serde_json::{Value, json};
use tokio::sync::Mutex;

const OWN_BROKER_ID: &str = "6099c63a8d75SCDE";

fn swap_instruments() -> Vec<InstrumentAny> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_data")
        .join("http_get_instruments_swap.json");
    let payload: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let response: OKXResponse<OKXInstrument> = serde_json::from_value(payload).unwrap();
    response
        .data
        .iter()
        .filter_map(|raw| parse_instrument_any(raw, None, None, UnixNanos::default()).ok().flatten())
        .collect()
}

#[tokio::test]
async fn test_broker_id_env_replaces_order_tag() {
    // SAFETY: the only test in this binary, and no other thread reads the environment yet.
    unsafe {
        std::env::set_var(OKX_BROKER_ID_ENV, OWN_BROKER_ID);
    }

    let captured: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let sink = captured.clone();
    let router = Router::new().route(
        "/api/v5/trade/order",
        post(move |_headers: HeaderMap, Json(payload): Json<Value>| {
            let sink = sink.clone();
            async move {
                let cl_ord_id = payload["clOrdId"].clone();
                *sink.lock().await = Some(payload);
                Json(json!({
                    "code": "0",
                    "msg": "",
                    "data": [{"ordId": "12345", "clOrdId": cl_ord_id, "sCode": "0", "sMsg": ""}],
                }))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router.into_make_service())
            .await
            .unwrap();
    });

    let client = OKXHttpClient::with_credentials(
        Some("test_key".to_string()),
        Some("test_secret".to_string()),
        Some("test_passphrase".to_string()),
        Some(format!("http://{addr}")),
        60,
        3,
        1000,
        10_000,
        OKXEnvironment::Live,
        None,
    )
    .unwrap();
    let instruments = swap_instruments();
    client.cache_instruments(&instruments);
    let instrument_id = InstrumentId::from("BTC-USDT-SWAP.OKX");
    assert!(instruments.iter().any(|i| i.id() == instrument_id));

    client
        .place_order_with_domain_types(
            instrument_id,
            OKXTradeMode::Isolated,
            ClientOrderId::from("Obrokertag1"),
            OrderSide::Buy,
            OrderType::Limit,
            Quantity::from("1.00"),
            Some(TimeInForce::Gtc),
            Some(Price::from("50000.0")),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let body = captured.lock().await.clone().expect("order body must be captured");
    assert_eq!(okx_broker_id(), OWN_BROKER_ID);
    assert_eq!(body["tag"], OWN_BROKER_ID);
    assert_ne!(body["tag"], OKX_NAUTILUS_BROKER_ID);
}
