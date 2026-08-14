use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "action")]
pub enum IncomingMessage {
    #[serde(rename = "register")]
    Register { task_type: String },
    #[serde(rename = "enqueue")]
    Enqueue {
        task_id: String,
        task_type: String,
        task_data: String,
        max_retries: i32,
        retry_after_hours: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        rate_limit_group: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_per_minute: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        auto_dedupe: Option<bool>,
    },
    #[serde(rename = "result")]
    Result {
        task_id: String,
        status: String,
        error_msg: Option<String>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "action")]
pub enum OutgoingMessage {
    #[serde(rename = "execute")]
    Execute {
        task_id: String,
        task_type: String,
        task_data: String,
    },
    #[serde(rename = "max_retries_reached")]
    MaxRetriesReached {
        task_id: String,
        task_type: String,
        task_data: String,
    },
    #[serde(rename = "ack")]
    Ack { #[serde(skip_serializing_if = "Option::is_none")] task_id: Option<String>, message: String },
    #[serde(rename = "error")]
    Error { #[serde(skip_serializing_if = "Option::is_none")] task_id: Option<String>, message: String },
}
