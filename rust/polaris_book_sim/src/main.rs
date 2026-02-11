use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::io::{self, BufRead, BufWriter, Read, Write};
use std::time::Instant;

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

#[derive(Debug, Deserialize)]
struct BenchPayload {
    iterations: Option<usize>,
    depth_pct: Option<f64>,
    initial_bids: Vec<Level>,
    initial_asks: Vec<Level>,
    updates: Vec<BenchUpdate>,
}

#[derive(Debug, Deserialize)]
struct BenchUpdate {
    side: String,
    price: f64,
    size: f64,
}

#[derive(Debug, Serialize)]
struct BenchOutput {
    iterations: usize,
    updates_per_iteration: usize,
    total_updates: usize,
    decode_ms: f64,
    process_ms: f64,
    total_ms: f64,
    avg_update_us: f64,
    p50_update_us: f64,
    p95_update_us: f64,
    p99_update_us: f64,
    max_update_us: f64,
    spread_avg: f64,
    bid_depth_1pct_avg: f64,
    ask_depth_1pct_avg: f64,
}

fn main() {
    let mut args = std::env::args().skip(1);
    let command = args.next().unwrap_or_default();
    match command.as_str() {
        "simulate-paper" => run_once(),
        "daemon" => run_daemon(),
        "bench-orderbook" => run_bench_orderbook(),
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

fn run_bench_orderbook() {
    let total_started = Instant::now();
    let mut raw = String::new();
    if io::stdin().read_to_string(&mut raw).is_err() {
        eprintln!("failed to read bench payload");
        std::process::exit(2);
    }

    let decode_started = Instant::now();
    let payload: BenchPayload = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(err) => {
            eprintln!("invalid bench payload: {}", err);
            std::process::exit(2);
        }
    };
    let decode_ms = decode_started.elapsed().as_secs_f64() * 1000.0;
    let output = run_bench(payload, decode_ms, total_started);
    match serde_json::to_string(&output) {
        Ok(text) => println!("{}", text),
        Err(err) => {
            eprintln!("failed to encode bench output: {}", err);
            std::process::exit(2);
        }
    }
}

fn run_bench(payload: BenchPayload, decode_ms: f64, total_started: Instant) -> BenchOutput {
    let iterations = payload.iterations.unwrap_or(100).max(1);
    let depth_pct = payload.depth_pct.unwrap_or(1.0).max(0.01);
    let updates_per_iteration = payload.updates.len();
    if updates_per_iteration == 0 {
        let total_ms = total_started.elapsed().as_secs_f64() * 1000.0;
        return BenchOutput {
            iterations,
            updates_per_iteration: 0,
            total_updates: 0,
            decode_ms: round6(decode_ms),
            process_ms: 0.0,
            total_ms: round6(total_ms),
            avg_update_us: 0.0,
            p50_update_us: 0.0,
            p95_update_us: 0.0,
            p99_update_us: 0.0,
            max_update_us: 0.0,
            spread_avg: 0.0,
            bid_depth_1pct_avg: 0.0,
            ask_depth_1pct_avg: 0.0,
        };
    }

    let seed_bids = to_book(&payload.initial_bids);
    let seed_asks = to_book(&payload.initial_asks);

    let mut update_latency_us: Vec<f64> = Vec::with_capacity(iterations * updates_per_iteration);
    let mut spread_sum = 0.0;
    let mut spread_count = 0usize;
    let mut bid_depth_sum = 0.0;
    let mut ask_depth_sum = 0.0;
    let mut depth_count = 0usize;

    let process_started = Instant::now();
    for _ in 0..iterations {
        let mut bids = seed_bids.clone();
        let mut asks = seed_asks.clone();
        for upd in &payload.updates {
            let tick_started = Instant::now();
            let side = upd.side.trim().to_ascii_uppercase();
            let tick = to_tick(upd.price);
            if tick > 0 {
                if side == "BID" {
                    apply_update(&mut bids, tick, upd.size);
                } else if side == "ASK" {
                    apply_update(&mut asks, tick, upd.size);
                }
            }

            let best_bid_tick = bids.keys().next_back().copied().unwrap_or(0);
            let best_ask_tick = asks.keys().next().copied().unwrap_or(0);
            if best_bid_tick > 0 && best_ask_tick > 0 {
                let best_bid = tick_to_price(best_bid_tick);
                let best_ask = tick_to_price(best_ask_tick);
                spread_sum += best_ask - best_bid;
                spread_count += 1;
                bid_depth_sum += depth_within_pct(&bids, best_bid_tick, depth_pct, true);
                ask_depth_sum += depth_within_pct(&asks, best_ask_tick, depth_pct, false);
                depth_count += 1;
            }
            update_latency_us.push(tick_started.elapsed().as_secs_f64() * 1_000_000.0);
        }
    }
    let process_ms = process_started.elapsed().as_secs_f64() * 1000.0;
    let total_ms = total_started.elapsed().as_secs_f64() * 1000.0;
    let max_update = update_latency_us
        .iter()
        .copied()
        .fold(0.0_f64, f64::max);

    BenchOutput {
        iterations,
        updates_per_iteration,
        total_updates: iterations * updates_per_iteration,
        decode_ms: round6(decode_ms),
        process_ms: round6(process_ms),
        total_ms: round6(total_ms),
        avg_update_us: round6(mean(&update_latency_us)),
        p50_update_us: round6(percentile(&update_latency_us, 50.0)),
        p95_update_us: round6(percentile(&update_latency_us, 95.0)),
        p99_update_us: round6(percentile(&update_latency_us, 99.0)),
        max_update_us: round6(max_update),
        spread_avg: round6(if spread_count > 0 { spread_sum / spread_count as f64 } else { 0.0 }),
        bid_depth_1pct_avg: round6(if depth_count > 0 { bid_depth_sum / depth_count as f64 } else { 0.0 }),
        ask_depth_1pct_avg: round6(if depth_count > 0 { ask_depth_sum / depth_count as f64 } else { 0.0 }),
    }
}

fn to_book(levels: &[Level]) -> BTreeMap<i64, f64> {
    let mut out = BTreeMap::new();
    for level in levels {
        let tick = to_tick(level.price);
        if tick <= 0 || level.size <= 0.0 {
            continue;
        }
        out.insert(tick, level.size);
    }
    out
}

fn apply_update(book: &mut BTreeMap<i64, f64>, tick: i64, size: f64) {
    if size <= 0.0 {
        book.remove(&tick);
    } else {
        book.insert(tick, size);
    }
}

fn depth_within_pct(book: &BTreeMap<i64, f64>, best_tick: i64, pct: f64, is_bid: bool) -> f64 {
    if best_tick <= 0 {
        return 0.0;
    }
    let best = tick_to_price(best_tick);
    let mut total = 0.0;
    for (tick, size) in book {
        let price = tick_to_price(*tick);
        let dist = if is_bid {
            ((best - price) / best) * 100.0
        } else {
            ((price - best) / best) * 100.0
        };
        if dist <= pct {
            total += *size;
        }
    }
    total
}

fn to_tick(price: f64) -> i64 {
    ((price * 1_000_000.0).round()) as i64
}

fn tick_to_price(tick: i64) -> f64 {
    tick as f64 / 1_000_000.0
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let sum: f64 = values.iter().sum();
    sum / values.len() as f64
}

fn percentile(values: &[f64], p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let pos = ((p / 100.0) * (sorted.len() as f64 - 1.0)).round();
    let idx = pos.max(0.0).min(sorted.len() as f64 - 1.0) as usize;
    sorted[idx]
}

fn round6(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
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
