// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Trade8:Desk
// SPDX-License-Identifier: Apache-2.0

//! Session clock - the env session state machine driven by
//! Sentin8:Cron proxy-back (preopen/open/close-pending/closed).
//!
//! Design: `../desk/DESIGN.md` §4.5.

use std::sync::atomic::{AtomicU8, Ordering};

use trade8::contracts::SessionState;

const PREOPEN: u8 = 0;
const OPEN: u8 = 1;
const CLOSE_PENDING: u8 = 2;
const CLOSED: u8 = 3;

/// Atomic session state - hot-path reads are a single load.
#[derive(Default)]
pub struct SessionClock {
    state: AtomicU8,
}

impl SessionClock {
    pub fn new(initial: SessionState) -> Self {
        SessionClock {
            state: AtomicU8::new(to_u8(initial)),
        }
    }

    pub fn state(&self) -> SessionState {
        from_u8(self.state.load(Ordering::Acquire))
    }

    pub fn set(&self, s: SessionState) {
        self.state.store(to_u8(s), Ordering::Release);
    }

    pub fn as_str(&self) -> &'static str {
        self.state().as_str()
    }
}

fn to_u8(s: SessionState) -> u8 {
    match s {
        SessionState::Preopen => PREOPEN,
        SessionState::Open => OPEN,
        SessionState::ClosePending => CLOSE_PENDING,
        SessionState::Closed => CLOSED,
    }
}

fn from_u8(v: u8) -> SessionState {
    match v {
        OPEN => SessionState::Open,
        CLOSE_PENDING => SessionState::ClosePending,
        CLOSED => SessionState::Closed,
        _ => SessionState::Preopen,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_transitions_and_gates() {
        let clock = SessionClock::default();
        assert_eq!(clock.state(), SessionState::Preopen);
        assert!(!clock.state().accepts_orders());

        clock.set(SessionState::Open);
        assert!(clock.state().accepts_orders());
        assert!(clock.state().accepts_cancels());

        clock.set(SessionState::ClosePending);
        assert!(!clock.state().accepts_orders());
        assert!(clock.state().accepts_cancels());

        clock.set(SessionState::Closed);
        assert!(!clock.state().accepts_orders());
        assert!(!clock.state().accepts_cancels());
        assert_eq!(clock.as_str(), "closed");
    }

    #[test]
    fn parse_roundtrip() {
        for s in ["preopen", "open", "close_pending", "closed"] {
            assert_eq!(SessionState::parse(s).unwrap().as_str(), s);
        }
        assert!(SessionState::parse("nope").is_none());
    }
}
