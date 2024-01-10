// Merge multiple arrays based on reorder
#[macro_export]
macro_rules! merge_arrays_inner {
    ($self:expr,$field:expr,$tmp_ty:path,$in_ty:ty, $out_ty:ty,$col:expr,$reorder:expr,$streams:expr) => {{
        let col_path: ColumnPath = $col.path_in_schema.clone();
        let mut buf = vec![];
        // check if column is exist in each stream
        let col_exist_per_stream = $self
            .page_streams
            .iter()
            .enumerate()
            .map(|(stream_id, stream)| {
                $streams.contains(&stream_id) && stream.contains_column(&col_path)
            })
            .collect::<Vec<bool>>();

        // pop arrays from tmp_arrays
        // used to downcast (TmpArray) and avoid borrow checker
        let mut arrs = $self
            .tmp_arrays
            .iter_mut()
            .enumerate()
            .map(|(stream_id, cols)| {
                if $streams.contains(&stream_id) {
                    match cols.remove(&col_path) {
                        None => None,
                        Some(v) => {
                            if let $tmp_ty(arr) = v {
                                Some(arr)
                            } else {
                                None
                            }
                        }
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // calculate array indexes
        let mut arrs_idx: Vec<usize> = $self
            .tmp_array_idx
            .iter()
            .map(|cols| match cols.get(&col_path) {
                Some(idx) => *idx,
                None => 0,
            })
            .collect();

        // make output array
        // perf: make reusable buffer
        let mut out = <$out_ty>::with_capacity($reorder.len());
        // go through each row idx in reorder, get stream id for row
        for idx in 0..$reorder.len() {
            let stream_id = $reorder[idx];
            // push null if column doesn't exist
            if !col_exist_per_stream[stream_id] {
                out.push_null();
                continue;
            }

            // downcast array if it is not exist in tmp
            if arrs[stream_id].is_none() {
                let page = $self.page_streams[stream_id].next_page(&col_path)?.unwrap();
                if let CompressedPage::Data(page) = page {
                    let any_arr = data_page_to_array(page, &$col, $field.clone(), &mut buf)?;
                    let tmp_arr = any_arr.as_any().downcast_ref::<$in_ty>().unwrap().clone();
                    arrs[stream_id] = Some(tmp_arr);
                    arrs_idx[stream_id] = 0;
                }
            }

            let cur_idx = arrs_idx[stream_id];
            let arr = arrs[stream_id].as_ref().unwrap();
            if arr.is_null(cur_idx) {
                out.push_null();
            } else {
                out.push(Some(arr.value(cur_idx)));
            }

            if cur_idx == arr.len() - 1 {
                arrs[stream_id] = None;
                arrs_idx[stream_id] = 0;
            } else {
                arrs_idx[stream_id] += 1;
            }
        }

        // move all arrays back to tmp
        for (stream_id, maybe_arr) in arrs.into_iter().enumerate() {
            if let Some(arr) = maybe_arr {
                $self.tmp_arrays[stream_id].insert(col_path.clone(), $tmp_ty(arr));
            }
        }

        for (stream_id, idx) in arrs_idx.into_iter().enumerate() {
            $self.tmp_array_idx[stream_id].insert(col_path.clone(), idx);
        }

        out
    }};
}

// merge data arrays
#[macro_export]
macro_rules! merge_arrays {
    ($self:expr,$field:expr,$tmp_ty:path,$in_ty:ty, $out_ty:ty,$col:expr,$reorder:expr,$streams:expr) => {{
        let mut out = merge_arrays_inner!(
            $self, $field, $tmp_ty, $in_ty, $out_ty, $col, $reorder, $streams
        );

        out.as_box()
    }};
}

// merge primitive arrays
#[macro_export]
macro_rules! merge_primitive_arrays {
    ($self:expr,$field:expr,$tmp_ty:path,$in_ty:ty, $out_ty:ty,$col:expr,$reorder:expr,$streams:expr) => {{
        let out = merge_arrays_inner!(
            $self, $field, $tmp_ty, $in_ty, $out_ty, $col, $reorder, $streams
        );

        out.to($field.data_type().to_owned()).as_box()
    }};
}

// merge list arrays
#[macro_export]
macro_rules! merge_list_arrays_inner {
    ($self:expr,$field:expr,$offset:ty, $tmp_ty:path,$in_ty:ty, $out_ty:ty,$col:expr,$reorder:expr,$streams:expr) => {{
        let col_path: ColumnPath = $col.path_in_schema.clone();
        let mut buf = vec![];
        let col_exist_per_stream = $self
            .page_streams
            .iter()
            .enumerate()
            .map(|(stream_id, stream)| {
                $streams.contains(&stream_id) && stream.contains_column(&col_path)
            })
            .collect::<Vec<bool>>();

        let mut arrs = $self
            .tmp_arrays
            .iter_mut()
            .enumerate()
            .map(|(stream_id, cols)| {
                if $streams.contains(&stream_id) {
                    match cols.remove(&col_path) {
                        None => None,
                        Some(v) => {
                            if let $tmp_ty(arr, offsets, validity, num_vals) = v {
                                Some((arr, offsets, validity, num_vals))
                            } else {
                                None
                            }
                        }
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let mut arrs_idx: Vec<usize> = $self
            .tmp_array_idx
            .iter()
            .map(|cols| match cols.get(&col_path) {
                Some(idx) => *idx,
                None => 0,
            })
            .collect();

        let mut out = <$out_ty>::with_capacity($reorder.len());
        for idx in 0..$reorder.len() {
            let stream_id = $reorder[idx];
            if !col_exist_per_stream[stream_id] {
                out.push_null();
                continue;
            }

            if arrs[stream_id].is_none() {
                let page = $self.page_streams[stream_id].next_page(&col_path)?.unwrap();
                if let CompressedPage::Data(page) = page {
                    let any_arr = data_page_to_array(page, &$col, $field.clone(), &mut buf)?;
                    let list_arr = any_arr
                        .as_any()
                        .downcast_ref::<ListArray<$offset>>()
                        .unwrap()
                        .clone();
                    let arr = list_arr
                        .values()
                        .as_any()
                        .downcast_ref::<$in_ty>()
                        .unwrap()
                        .clone();
                    let offsets = list_arr.offsets().clone();
                    let validity = list_arr.validity().map(|v| v.clone());
                    arrs[stream_id] = Some((arr, offsets, validity, list_arr.len()));
                    arrs_idx[stream_id] = 0;
                }
            }

            let cur_idx = arrs_idx[stream_id];
            let (arr, offsets, validity, num_vals) = arrs[stream_id].as_ref().unwrap();
            if validity
                .as_ref()
                .map(|x| !x.get_bit(cur_idx))
                .unwrap_or(false)
            {
                out.push_null();
            } else {
                let (start, end) = offsets.start_end(cur_idx);
                let length = end - start;
                // FIXME avoid clone?
                let vals = arr.clone().sliced(start, length);
                out.try_push(Some(vals.into_iter()))?;
            }

            if cur_idx == *num_vals - 1 {
                arrs[stream_id] = None;
                arrs_idx[stream_id] = 0;
            } else {
                arrs_idx[stream_id] += 1;
            }
        }

        for (stream_id, maybe_arr) in arrs.into_iter().enumerate() {
            if let Some((a, o, v, l)) = maybe_arr {
                $self.tmp_arrays[stream_id].insert(col_path.clone(), $tmp_ty(a, o, v, l));
            }
        }

        for (stream_id, idx) in arrs_idx.into_iter().enumerate() {
            $self.tmp_array_idx[stream_id].insert(col_path.clone(), idx);
        }

        out
    }};
}

#[macro_export]
macro_rules! merge_list_arrays {
    ($self:expr,$field:expr,$offset:ty, $tmp_ty:path,$in_ty:ty, $out_ty:ty,$col:expr,$reorder:expr,$streams:expr) => {{
        let mut out = merge_list_arrays_inner!(
            $self, $field, $offset, $tmp_ty, $in_ty, $out_ty, $col, $reorder, $streams
        );

        out.as_box()
    }};
}

#[macro_export]
macro_rules! merge_list_primitive_arrays {
    ($self:expr,$field:expr,$inner_field:expr,$offset:ty, $tmp_ty:path,$in_ty:ty, $out_ty:ty,$col:expr,$reorder:expr,$streams:expr) => {{
        let col_path: ColumnPath = $col.path_in_schema.clone();
        let mut buf = vec![];
        let col_exist_per_stream = $self
            .page_streams
            .iter()
            .enumerate()
            .map(|(stream_id, stream)| {
                $streams.contains(&stream_id) && stream.contains_column(&col_path)
            })
            .collect::<Vec<bool>>();

        let mut arrs = $self
            .tmp_arrays
            .iter_mut()
            .enumerate()
            .map(|(stream_id, cols)| {
                if $streams.contains(&stream_id) {
                    match cols.remove(&col_path) {
                        None => None,
                        Some(v) => {
                            if let $tmp_ty(arr, offsets, validity, num_vals) = v {
                                Some((arr, offsets, validity, num_vals))
                            } else {
                                None
                            }
                        }
                    }
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let mut arrs_idx: Vec<usize> = $self
            .tmp_array_idx
            .iter()
            .map(|cols| match cols.get(&col_path) {
                Some(idx) => *idx,
                None => 0,
            })
            .collect();

        let mut out = <$out_ty>::with_capacity($reorder.len());
        for idx in 0..$reorder.len() {
            let stream_id = $reorder[idx];
            if !col_exist_per_stream[stream_id] {
                out.push_null();
                continue;
            }

            if arrs[stream_id].is_none() {
                let page = $self.page_streams[stream_id].next_page(&col_path)?.unwrap();
                if let CompressedPage::Data(page) = page {
                    let any_arr = data_page_to_array(page, &$col, $field.clone(), &mut buf)?;
                    let list_arr = any_arr
                        .as_any()
                        .downcast_ref::<ListArray<$offset>>()
                        .unwrap()
                        .clone();
                    let arr = list_arr
                        .values()
                        .as_any()
                        .downcast_ref::<$in_ty>()
                        .unwrap()
                        .clone()
                        .to($inner_field.data_type().clone());
                    let offsets = list_arr.offsets().clone();
                    let validity = list_arr.validity().map(|v| v.clone());
                    arrs[stream_id] = Some((arr, offsets, validity, list_arr.len()));
                    arrs_idx[stream_id] = 0;
                }
            }

            let cur_idx = arrs_idx[stream_id];
            let (arr, offsets, validity, num_vals) = arrs[stream_id].as_ref().unwrap();
            if validity
                .as_ref()
                .map(|x| !x.get_bit(cur_idx))
                .unwrap_or(false)
            {
                out.push_null();
            } else {
                let (start, end) = offsets.start_end(cur_idx);
                let length = end - start;
                // FIXME avoid clone?
                let vals = arr.clone().sliced(start, length);
                out.try_push(Some(vals.into_iter()))?;
            }

            if cur_idx == *num_vals - 1 {
                arrs[stream_id] = None;
                arrs_idx[stream_id] = 0;
            } else {
                arrs_idx[stream_id] += 1;
            }
        }

        for (stream_id, maybe_arr) in arrs.into_iter().enumerate() {
            if let Some((a, o, v, l)) = maybe_arr {
                $self.tmp_arrays[stream_id].insert(col_path.clone(), $tmp_ty(a, o, v, l));
            }
        }

        for (stream_id, idx) in arrs_idx.into_iter().enumerate() {
            $self.tmp_array_idx[stream_id].insert(col_path.clone(), idx);
        }

        out.as_box()
    }};
}
