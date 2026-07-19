mod support;

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;

use bolt_protocol::{
    BOLT_MAGIC, BoltVersion, ChunkDecoder, ClientMessage, Value, decode, encode_chunks,
    encode_client_message,
};
use bolt_server::{BoltConnectionConfig, serve_connection};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use support::FakeService;

#[tokio::test]
async fn serves_handshake_hello_run_and_pull_over_chunked_bytes() {
    let (mut client, mut server) = tokio::io::duplex(64 << 10);
    let service = Arc::new(FakeService::default());
    let task = tokio::spawn(async move {
        serve_connection(&mut server, service, BoltConnectionConfig::default()).await
    });

    let mut handshake = Vec::from(BOLT_MAGIC.to_be_bytes());
    handshake.extend_from_slice(&BoltVersion::new(5, 8, 0).encode());
    handshake.extend_from_slice(&[0; 12]);
    client.write_all(&handshake).await.expect("handshake");
    let mut selected = [0; 4];
    client.read_exact(&mut selected).await.expect("selection");
    assert_eq!(selected, BoltVersion::new(5, 8, 0).encode());
    let mut receiver = Receiver::new();

    send(
        &mut client,
        ClientMessage::Hello(BTreeMap::from([(
            "user_agent".into(),
            Value::String("dtg-test".into()),
        )])),
    )
    .await;
    let hello = receiver.receive(&mut client).await;
    assert!(matches!(
        decode(&hello).expect("hello response"),
        Value::Structure {
            signature: 0x70,
            ..
        }
    ));

    send(
        &mut client,
        ClientMessage::Run {
            query: "RETURN 1 AS value".into(),
            parameters: BTreeMap::new(),
            extra: BTreeMap::new(),
        },
    )
    .await;
    assert!(matches!(
        decode(&receiver.receive(&mut client).await).expect("run response"),
        Value::Structure {
            signature: 0x70,
            ..
        }
    ));
    send(
        &mut client,
        ClientMessage::Pull {
            n: -1,
            query_id: None,
        },
    )
    .await;
    assert!(matches!(
        decode(&receiver.receive(&mut client).await).expect("record"),
        Value::Structure {
            signature: 0x71,
            ..
        }
    ));
    assert!(matches!(
        decode(&receiver.receive(&mut client).await).expect("summary"),
        Value::Structure {
            signature: 0x70,
            ..
        }
    ));

    send(&mut client, ClientMessage::Goodbye).await;
    drop(client);
    task.await.expect("connection task").expect("connection");
}

async fn send(stream: &mut tokio::io::DuplexStream, message: ClientMessage) {
    let payload = encode_client_message(&message).expect("message");
    let framed = encode_chunks(&payload, 32).expect("chunks");
    stream.write_all(&framed).await.expect("write");
}

struct Receiver {
    decoder: ChunkDecoder,
    pending: VecDeque<Vec<u8>>,
}

impl Receiver {
    fn new() -> Self {
        Self {
            decoder: ChunkDecoder::new(1 << 20, u16::MAX.into()).expect("decoder"),
            pending: VecDeque::new(),
        }
    }

    async fn receive(&mut self, stream: &mut tokio::io::DuplexStream) -> Vec<u8> {
        let mut buffer = [0; 1_024];
        loop {
            if let Some(message) = self.pending.pop_front() {
                return message;
            }
            let read = stream.read(&mut buffer).await.expect("read");
            assert_ne!(read, 0, "connection closed before a response");
            self.pending
                .extend(self.decoder.push(&buffer[..read]).expect("framing"));
        }
    }
}
