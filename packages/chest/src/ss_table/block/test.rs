use std::io::Cursor;

use super::{block_builder::*, block_decoder::BlockDecoder};

#[test]
fn test_block_encode_decode() {
    let mut block = BlockBuilder::new();
    block.push("app".to_string(), "new".to_string()).unwrap();
    block.push("apple".to_string(), "Red".to_string()).unwrap();
    block
        .push("applet".to_string(), "something".to_string())
        .unwrap();

    let encoded = block.encode();

    let encoded_len = encoded.len();
    let re_decoded = BlockDecoder::decode(Cursor::new(encoded), encoded_len).unwrap();
    assert!(!re_decoded.restart_offsets().is_empty());
    assert!(!re_decoded.restart_offsets_count() > 0);
}
