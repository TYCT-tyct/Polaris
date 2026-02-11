use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::io::{self, BufRead, BufWriter, Read, Write};

#[derive(Debug, Deserialize)]
struct InputPayload {
    slippage_bps: u32,
    intents: Vec<IntentIn>,
    snapshots: HashMap<String, SnapshotIn>,
}

#[derive(Debug, Deserialize)]
struct RequestEnvelope {
    id: String,
    payload: InputPayload,
}

#[derive(Debug, Deserialize)]
struct IntentIn {
    intent_id: String,
    token_id: String,
    market_id: String,
    side: String,
    order_type: String,
    limit_price: Option<f64>,
    shares: Option<f64>,
    notional_usd: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct SnapshotIn {
    market_id: String,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    min_order_size: Option<f64>,
    tick_size: Option<f64>,
    bids: Vec<Level>,
    asks: Vec<Level>,
}

#[derive(Debug, Deserialize, Clone, Copy)]
struct Level {
    price: f64,
    size: f64,
}

#[derive(Debug, Serialize)]
struct OutputPayload {
    fills: Vec<FillOut>,
    events: Vec<EventOut>,
    capital_used_usd: f64,
}

#[derive(Debug, Serialize)]
struct FillOut {
    token_id: String,
    market_id: String,
    side: String,
    fill_price: f64,
    fill_size: f64,
    fill_notional_usd: f64,
    fee_usd: f64,
}

#[derive(Debug, Serialize)]
struct EventOut {
    intent_id: String,
    event_type: String,
    status_code: i32,
    message: String,
    payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ResponseEnvelope {
    id: String,
    ok: bool,
    result: Option<OutputPayload>,
    error: Option<String>,
}

#[derive(Debug)]
struct SimFill {
    avg_price: f64,
    filled_size: f64,
    notional: f64,
}

fn main() {
    let mut args = std::env::args().skip(1);
    let command = args.next().unwrap_or_default();
    match command.as_str() {
        "simulate-paper" => run_once(),
        "daemon" => run_daemon(),
        _ => {
            eprintln!("unsupported command: {}", command);
            std::process::exit(2);
        }
    }
}

fn run_once() {
    let mut raw = String::new();
    if io::stdin().read_to_string(&mut raw).is_err() {
        eprintln!("failed to read stdin");
        std::process::exit(2);
    }
    let payload: InputPayload = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(err) => {
            eprintln!("invalid json: {}", err);
            std::process::exit(2);
        }
    };

    let output = simulate(payload);
    match serde_json::to_string(&output) {
        Ok(text) => println!("{}", text),
        Err(err) => {
            eprintln!("failed to encode result: {}", err);
            std::process::exit(2);
        }
    }
}

fn run_daemon() {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut writer = BufWriter::new(stdout.lock());

    for raw in stdin.lock().lines() {
        let line = match raw {
            Ok(v) => v,
            Err(err) => {
                eprintln!("failed to read request line: {}", err);
                break;
            }
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let req: RequestEnvelope = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(err) => {
                let _ = write_response(
                    &mut writer,
                    ResponseEnvelope {
                        id: "unknown".to_string(),
                        ok: false,
                        result: None,
                        error: Some(format!("invalid_request: {}", err)),
                    },
                );
                continue;
            }
        };

        let output = simulate(req.payload);
        if write_response(
            &mut writer,
            ResponseEnvelope {
                id: req.id,
                ok: true,
                result: Some(output),
                error: None,
            },
        )
        .is_err()
        {
            break;
        }
    }
}

fn write_response(writer: &mut BufWriter<io::StdoutLock<'_>>, envelope: ResponseEnvelope) -> io::Result<()> {
    let text = serde_json::to_string(&envelope)
        .unwrap_or_else(|_| "{\"id\":\"unknown\",\"ok\":false,\"result\":null,\"error\":\"encode_failed\"}".to_string());
    writer.write_all(text.as_bytes())?;
    writer.write_all(b"\n")?;
    writer.flush()
}

fn simulate(payload: InputPayload) -> OutputPayload {
    let mut fills: Vec<FillOut> = Vec::new();
    let mut events: Vec<EventOut> = Vec::new();
    let mut capital_used = 0.0_f64;

    for intent in payload.intents {
        let snap = match payload.snapshots.get(&intent.token_id) {
            Some(v) => v,
            None => {
                events.push(EventOut {
                    intent_id: intent.intent_id,
                    event_type: "reject".to_string(),
                    status_code: 404,
                    message: "missing_snapshot".to_string(),
                    payload: serde_json::json!({}),
                });
                continue;
            }
        };

        let side = intent.side.trim().to_ascii_uppercase();
        let order_type = intent.order_type.trim().to_ascii_uppercase();
        let limit_price = resolve_price(intent.limit_price, &side, snap);
        let shares = resolve_shares(intent.shares, intent.notional_usd, limit_price, snap);
        if shares <= 0.0 {
            events.push(EventOut {
                intent_id: intent.intent_id,
                event_type: "reject".to_string(),
                status_code: 422,
                message: "invalid_size".to_string(),
                payload: serde_json::json!({}),
            });
            continue;
        }
        if limit_price <= 0.0 {
            events.push(EventOut {
                intent_id: intent.intent_id,
                event_type: "reject".to_string(),
                status_code: 422,
                message: "invalid_price".to_string(),
                payload: serde_json::json!({}),
            });
            continue;
        }

        let normalized = normalize_order(limit_price, shares, &side, snap);
        let (price, wanted) = match normalized {
            Some(v) => v,
            None => {
                events.push(EventOut {
                    intent_id: intent.intent_id,
                    event_type: "reject".to_string(),
                    status_code: 422,
                    message: "below_min_order_size".to_string(),
                    payload: serde_json::json!({}),
                });
                continue;
            }
        };

        if !preflight_ok(&side, price, snap, payload.slippage_bps) {
            events.push(EventOut {
                intent_id: intent.intent_id,
                event_type: "reject".to_string(),
                status_code: 422,
                message: "preflight_price_drift".to_string(),
                payload: serde_json::json!({
                    "limit_price": price,
                    "best_bid": snap.best_bid,
                    "best_ask": snap.best_ask,
                }),
            });
            continue;
        }

        let allow_partial = matches!(order_type.as_str(), "FAK" | "IOC" | "PAPER");
        let sim = if side == "BUY" {
            simulate_buy_fill(&snap.asks, wanted, price, allow_partial)
        } else {
            simulate_sell_fill(&snap.bids, wanted, price, allow_partial)
        };
        let sim = match sim {
            Some(v) => v,
            None => {
                events.push(EventOut {
                    intent_id: intent.intent_id,
                    event_type: "reject".to_string(),
                    status_code: 422,
                    message: "insufficient_liquidity".to_string(),
                    payload: serde_json::json!({}),
                });
                continue;
            }
        };

        let event_type = if sim.filled_size + 1e-12 < wanted {
            "partial_fill"
        } else {
            "fill"
        };
        events.push(EventOut {
            intent_id: intent.intent_id,
            event_type: event_type.to_string(),
            status_code: 200,
            message: if event_type == "partial_fill" {
                "paper_partial_fill".to_string()
            } else {
                "paper_fill".to_string()
            },
            payload: serde_json::json!({
                "price": sim.avg_price,
                "size": sim.filled_size,
                "requested_size": wanted,
                "notional": sim.notional
            }),
        });
        fills.push(FillOut {
            token_id: intent.token_id,
            market_id: if intent.market_id.is_empty() {
                snap.market_id.clone()
            } else {
                intent.market_id
            },
            side: side.clone(),
            fill_price: sim.avg_price,
            fill_size: sim.filled_size,
            fill_notional_usd: sim.notional,
            fee_usd: 0.0,
        });
        capital_used += sim.notional;
    }

    OutputPayload {
        fills,
        events,
        capital_used_usd: capital_used,
    }
}

fn resolve_price(limit_price: Option<f64>, side: &str, snap: &SnapshotIn) -> f64 {
    if let Some(v) = limit_price {
        return v;
    }
    if side == "BUY" {
        return snap.best_ask.unwrap_or(0.0);
    }
    snap.best_bid.unwrap_or(0.0)
}

fn resolve_shares(shares: Option<f64>, notional: Option<f64>, price: f64, snap: &SnapshotIn) -> f64 {
    let min_size = snap.min_order_size.unwrap_or(0.0).max(0.0);
    if let Some(v) = shares {
        return v.max(min_size);
    }
    if let Some(v) = notional {
        if v > 0.0 && price > 0.0 {
            return (v / price).max(min_size);
        }
    }
    0.0
}

fn normalize_order(price: f64, shares: f64, side: &str, snap: &SnapshotIn) -> Option<(f64, f64)> {
    if price <= 0.0 || shares <= 0.0 {
        return None;
    }
    let min_size = snap.min_order_size.unwrap_or(0.0).max(0.0);
    let normalized_shares = shares.max(min_size);
    if normalized_shares <= 0.0 {
        return None;
    }
    let mut normalized_price = price;
    if let Some(tick) = snap.tick_size {
        if tick > 0.0 {
            let ratio = price / tick;
            let steps = if side == "BUY" { ratio.ceil() } else { ratio.floor() };
            normalized_price = (steps * tick).max(tick);
        }
    }
    Some((normalized_price, normalized_shares))
}

fn preflight_ok(side: &str, intended_price: f64, snap: &SnapshotIn, slippage_bps: u32) -> bool {
    let ratio = slippage_bps as f64 / 10_000.0_f64;
    if side == "BUY" {
        if let Some(best_ask) = snap.best_ask {
            return best_ask <= intended_price * (1.0 + ratio);
        }
        return false;
    }
    if let Some(best_bid) = snap.best_bid {
        return best_bid >= intended_price * (1.0 - ratio);
    }
    false
}

fn simulate_buy_fill(levels: &[Level], target_size: f64, limit_price: f64, allow_partial: bool) -> Option<SimFill> {
    if target_size <= 0.0 {
        return None;
    }
    let mut sorted = levels.to_vec();
    sorted.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(Ordering::Equal));
    simulate_fill(&sorted, target_size, limit_price, allow_partial, true)
}

fn simulate_sell_fill(levels: &[Level], target_size: f64, limit_price: f64, allow_partial: bool) -> Option<SimFill> {
    if target_size <= 0.0 {
        return None;
    }
    let mut sorted = levels.to_vec();
    sorted.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(Ordering::Equal));
    simulate_fill(&sorted, target_size, limit_price, allow_partial, false)
}

fn simulate_fill(
    levels: &[Level],
    target_size: f64,
    limit_price: f64,
    allow_partial: bool,
    is_buy: bool,
) -> Option<SimFill> {
    let mut remaining = target_size;
    let mut filled = 0.0_f64;
    let mut notional = 0.0_f64;

    for level in levels {
        let level_price = level.price;
        if is_buy && level_price > limit_price + 1e-12 {
            break;
        }
        if !is_buy && level_price + 1e-12 < limit_price {
            break;
        }
        if level.size <= 0.0 {
            continue;
        }
        let take = remaining.min(level.size);
        if take <= 0.0 {
            continue;
        }
        filled += take;
        notional += take * level_price;
        remaining -= take;
        if remaining <= 1e-12 {
            break;
        }
    }
    if filled <= 0.0 {
        return None;
    }
    if !allow_partial && (filled + 1e-12 < target_size) {
        return None;
    }
    Some(SimFill {
        avg_price: notional / filled,
        filled_size: filled,
        notional,
    })
}
