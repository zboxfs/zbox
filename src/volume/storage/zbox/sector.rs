
/// Space
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Space {
    pub(super) txid: Txid,
    pub(super) spans: SpanList,
}

impl Space {
    pub fn new(txid: Txid, spans: SpanList) -> Self {
        Space { txid, spans }
    }

