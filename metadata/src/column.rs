pub fn make_col_id_seq_key<'a>(project_id: u64) -> Vec<u8> {
    [b"columns/", project_id.to_le_bytes().as_ref(), b"/seq/id"].concat()
}