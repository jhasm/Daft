use std::{fs::OpenOptions, ops::Range, sync::Arc};

use bytes::Bytes;
use daft_io::{GetResult, IOClient, IOStatsRef};
use futures::{stream::BoxStream, Stream, StreamExt};

struct BytesConsumer {
    start: usize,
    end: usize,
    curr: usize,
    channel: tokio::sync::mpsc::UnboundedSender<Bytes>,
}

impl PartialEq for BytesConsumer {
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start && self.end == other.end && self.curr == other.curr
    }
}

impl Eq for BytesConsumer {}

impl Ord for BytesConsumer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .curr
            .cmp(&self.curr)
            .then_with(|| other.start.cmp(&self.start))
            .then_with(|| other.end.cmp(&self.end))
    }
}

impl PartialOrd for BytesConsumer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct InFlightState {
    consumers: std::collections::BinaryHeap<BytesConsumer>,
}

enum DownloaderState {
    InFlight(InFlightState),
    Complete(Vec<usize>, Vec<Bytes>),
}

impl DownloaderState {
    fn new() -> ProtectedState {
        ProtectedState(
            std::sync::Mutex::new(DownloaderState::InFlight(InFlightState {
                consumers: std::collections::BinaryHeap::new(),
            }))
            .into(),
        )
    }
}

#[derive(Clone)]
pub(crate) struct ProtectedState(Arc<std::sync::Mutex<DownloaderState>>);

impl ProtectedState {
    pub async fn get_stream(
        &self,
        range: Range<usize>,
    ) -> BoxStream<'static, std::result::Result<Bytes, daft_io::Error>> {
        let mut _g = self.0.lock().unwrap();
        match &mut (*_g) {
            DownloaderState::Complete(so, v) => {
                let mut current_pos = range.start;
                let mut curr_index;
                let start_point = so.binary_search(&current_pos);
                let mut bytes = vec![];
                match start_point {
                    Ok(index) => {
                        let index_pos = so[index];
                        let chunk = &v[index];
                        let end_offset = chunk.len().min(range.end - current_pos);
                        let start_offset = current_pos - index_pos;
                        bytes.push(chunk.slice(start_offset..end_offset));
                        assert_eq!(index_pos, current_pos);
                        current_pos += (end_offset - current_pos);
                        curr_index = index + 1;
                    }
                    Err(index) => {
                        assert!(index > 0, "range: {range:?}, start: {}", &so[index],);
                        let index = index - 1;
                        let index_pos = so[index];
                        assert!(index_pos < range.start);

                        let chunk = &v[index];
                        let start_offset = current_pos - index_pos;
                        let end_offset = chunk.len().min(range.end - index_pos);
                        assert!(current_pos >= range.start && current_pos < range.end, "range: {range:?}, current_pos: {current_pos}, bytes_start: {}, end: {}", range.start, range.end);

                        bytes.push(chunk.slice(start_offset..end_offset));
                        current_pos += (end_offset - start_offset);
                        curr_index = index + 1;
                    }
                };
                while current_pos < range.end && curr_index < v.len() {
                    let chunk = &v[curr_index];
                    let start_offset = 0;
                    let end_offset = chunk.len().min(range.end - current_pos);
                    bytes.push(chunk.slice(start_offset..end_offset));
                    current_pos += end_offset - start_offset;
                    curr_index += 1;
                }
                assert!(current_pos == range.end);

                assert_eq!(bytes.iter().map(|b| b.len()).sum::<usize>(), range.len());
                futures::stream::iter(bytes).map(|b| Ok(b)).boxed()
            }
            DownloaderState::InFlight(InFlightState { ref mut consumers }) => {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                let new_consumer = BytesConsumer {
                    start: range.start,
                    end: range.end,
                    curr: range.start,
                    channel: tx,
                };
                let expected_bytes = range.len();
                consumers.push(new_consumer);
                let stream = async_stream::stream! {
                    let mut bytes_read = 0;
                    while bytes_read < expected_bytes && let Some(v) = rx.recv().await {
                        bytes_read += v.len();
                        yield Ok(v)
                    }
                };
                stream.boxed()
            }
        }
    }
}

pub(crate) fn start_worker(
    url: String,
    global_range: Range<usize>,
    io_client: Arc<IOClient>,
    io_stats: Option<IOStatsRef>,
) -> ProtectedState {
    let state = DownloaderState::new();

    let owned_state = state.clone();
    let join_handle = tokio::spawn(async move {
        let expected_len = global_range.len();
        let expected_chunks = (expected_len + 4096 - 1) / 4096;

        let get_result = io_client
            .single_url_get(url, Some(global_range.clone()), io_stats)
            .await
            .unwrap();
        // dont unwrap
        let mut chunks = Vec::<Bytes>::with_capacity(expected_chunks);
        let mut start_offsets = Vec::<usize>::with_capacity(expected_chunks);
        let mut curr_pos = 0;
        let mut stream_active = true;
        if let GetResult::Stream(mut stream, ..) = get_result {
            loop {
                if stream_active && let Some(chunk) = stream.next().await {
                    let chunk = chunk.unwrap();
                    // dont unwrap
                    start_offsets.push(curr_pos);
                    curr_pos += chunk.len();
                    chunks.push(chunk);
                } else {
                    stream_active = false;
                }
                {
                    let mut _g = owned_state.0.lock().unwrap();
                    if let DownloaderState::InFlight(InFlightState { ref mut consumers }) =
                        &mut (*_g)
                    {
                        while !consumers.is_empty() && consumers.peek().unwrap().curr < curr_pos {
                            let mut c = consumers.pop().unwrap();
                            assert!(
                                c.curr >= c.start && c.curr <= c.end,
                                "start: {} curr: {} end: {} ",
                                c.start,
                                c.curr,
                                c.end
                            );
                            let start_point = start_offsets.binary_search(&c.curr);
                            let mut curr_index;
                            match start_point {
                                Ok(index) => {
                                    let index_pos = start_offsets[index];
                                    let chunk = &chunks[index];
                                    let end_offset = chunk.len().min(c.end - c.curr);
                                    let start_offset = c.curr - index_pos;
                                    let _v = c.channel.send(chunk.slice(start_offset..end_offset));
                                    assert_eq!(index_pos, c.curr);
                                    // let before = c.curr;
                                    c.curr += (end_offset - start_offset);
                                    // println!("ok before {before} after {}", c.curr);
                                    curr_index = index + 1;
                                }
                                Err(index) => {
                                    assert!(index > 0,);
                                    let index = index - 1;
                                    let index_pos = start_offsets[index];
                                    assert!(index_pos < c.curr);

                                    let chunk = &chunks[index];
                                    let start_offset = c.curr - index_pos;
                                    let end_offset = chunk.len().min(c.end - index_pos);
                                    assert!(
                                        c.curr >= c.start && c.curr < c.end,
                                        "start: {} curr: {} end: {} ",
                                        c.start,
                                        c.curr,
                                        c.end
                                    );
                                    let _v = c.channel.send(chunk.slice(start_offset..end_offset));
                                    // let before = c.curr;
                                    c.curr += (end_offset - start_offset);
                                    // println!("err before {before} after {}", c.curr);
                                    curr_index = index + 1;
                                }
                            };
                            while c.curr < curr_pos && curr_index < chunks.len() {
                                let chunk = &chunks[curr_index];
                                let start_offset = 0;
                                let end_offset = chunk.len().min(c.end - c.curr);
                                let _v = c.channel.send(chunk.slice(start_offset..end_offset));
                                // let before = c.curr;
                                c.curr += (end_offset - start_offset);
                                // println!("while before {before} after {}", c.curr);
                                curr_index += 1;
                            }
                            if c.curr < c.end {
                                consumers.push(c)
                            }
                        }
                        if !stream_active && consumers.is_empty() {
                            *_g = DownloaderState::Complete(start_offsets, chunks);
                            break;
                        }
                    }
                }
            }
        } else {
            unreachable!("we shouldn't be using this worker for local files")
        }
    });
    return state;
}
