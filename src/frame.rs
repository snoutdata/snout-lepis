//! Message framing that survives `tokio::select!`.
//!
//! The router waits on the client and on several nodes at once. `wire::read_message` reads with
//! `read_exact`, which loses whatever it had read if the select picks another branch; this reader
//! keeps its bytes in its own buffer and fills it with `read_buf`, which is cancellation safe, so
//! a message is never torn.

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::wire::{Message, WireError};

/// Postgres's own ceiling for one message.
pub const MAX_MESSAGE_LEN: usize = 1 << 30;
/// How far ahead of the bytes received the buffer grows for a long message.
const RESERVE_STEP: usize = 1 << 20;

pub struct FrameReader<R> {
	inner: R,
	buf: BytesMut,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
	pub fn new(inner: R) -> Self {
		FrameReader {
			inner,
			buf: BytesMut::with_capacity(16 * 1024),
		}
	}

	/// The stream underneath, for writing.
	pub fn get_mut(&mut self) -> &mut R {
		&mut self.inner
	}

	/// The next whole message. Cancellation safe.
	pub async fn next(&mut self) -> Result<Message, WireError> {
		loop {
			if let Some(m) = self.take()? {
				return Ok(m);
			}
			let n = self.inner.read_buf(&mut self.buf).await?;
			if n == 0 {
				return Err(WireError::Io(std::io::Error::new(
					std::io::ErrorKind::UnexpectedEof,
					"connection closed",
				)));
			}
		}
	}

	/// A whole message already buffered, if there is one; never waits.
	pub fn take(&mut self) -> Result<Option<Message>, WireError> {
		if self.buf.len() < 5 {
			return Ok(None);
		}
		let len = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;
		if !(4..=MAX_MESSAGE_LEN).contains(&len) {
			return Err(WireError::Protocol(format!(
				"message '{}' of {len} bytes",
				char::from(self.buf[0])
			)));
		}
		if self.buf.len() < len + 1 {
			// Room for what is announced, but never more than RESERVE_STEP ahead of the bytes
			// that have arrived: a header claiming 1 GB must not allocate 1 GB before the peer
			// has sent any of it (one such allocation failing aborts the process).
			self.buf
				.reserve((len + 1 - self.buf.len()).min(RESERVE_STEP));
			return Ok(None);
		}
		let tag = self.buf[0];
		self.buf.advance(5);
		let body = self.buf.split_to(len - 4).to_vec();
		Ok(Some(Message { tag, body }))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn reads_messages_split_across_reads() {
		let a = Message::new(b'Q', b"select 1\0".to_vec()).encode();
		let b = Message::new(b'S', vec![]).encode();
		let mut all = a.clone();
		all.extend_from_slice(&b);
		let (mut tx, rx) = tokio::io::duplex(3);
		let writer = tokio::spawn(async move {
			use tokio::io::AsyncWriteExt;
			for chunk in all.chunks(2) {
				tx.write_all(chunk).await.unwrap();
			}
		});
		let mut r = FrameReader::new(rx);
		assert_eq!(r.next().await.unwrap().tag, b'Q');
		assert_eq!(r.next().await.unwrap().tag, b'S');
		writer.await.unwrap();
	}

	#[tokio::test]
	async fn a_long_header_does_not_allocate_ahead_of_its_bytes() {
		let mut header = vec![b'Q'];
		header.extend_from_slice(&(MAX_MESSAGE_LEN as u32).to_be_bytes());
		let mut r = FrameReader::new(&header[..]);
		assert!(r.next().await.is_err());
		assert!(r.buf.capacity() <= 16 * 1024 + RESERVE_STEP);
	}
}
