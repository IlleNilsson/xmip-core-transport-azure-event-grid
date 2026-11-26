//! What a Location of this technology is configured with: its declaration,
//! which `Configured` reads, built with the crate's `declaration` feature
//! alone and the code behind the default (ADR-0064, amendments 2026-10-22
//! and 2026-11-26), for the library the surfaces load to carry (open
//! problem 48).

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, the topic endpoint a
/// Send Location publishes to,
/// `https://<topic>.<region>-1.eventgrid.azure.net/api/events`. The topic
/// key is the Location's `secret`, a password secret the Administration
/// Database keeps, never a setting.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[
        Setting {
            name: "listen",
            kind: Kind::Address,
            presence: Presence::Required,
            meaning: "Where a Receive Location listens for deliveries: the host and port \
                      the subscription's webhook URL resolves to.",
            applies: Applies::Receive,
        },
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long an endpoint that stops answering is waited on; unbounded \
             when left out.",
        ),
    ],
};

/// The schemes a Location's address is entered with to choose this technology.
pub const SCHEMES: &[&str] = &["azure-event-grid"];
