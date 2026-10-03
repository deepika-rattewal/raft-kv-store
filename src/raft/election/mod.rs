pub mod log_freshness;
pub mod state;
pub mod timeout;
pub mod vote;
pub mod vote_decision;

pub use log_freshness::{LogPosition, is_at_least_as_up_to_date};
pub use state::ElectionState;
pub use timeout::ElectionTimeout;
pub use vote::VoteTracker;
pub use vote_decision::{VoteDecision, decide_vote};
