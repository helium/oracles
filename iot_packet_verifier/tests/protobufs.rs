/// Loading & processing of encoded Protobufs and bypass AWS S3 during ingest.
#[cfg(test)]
mod test {
    extern crate iot_packet_verifier as pv;

    use helium_proto::services::router::PacketRouterPacketReportV1;
    use helium_proto::Message;
    use helium_proto::{GatewayBookkeeping, IotPacketVerifierBookkeeping, OuiBookkeeping};

    // https://developers.google.com/protocol-buffers/docs/encoding#structure
    static ENCODED_PROTOBUF_BYTES: [[u8; 66]; 6] = [
        [
            8, 229, 153, 138, 206, 177, 48, 16, 10, 32, 35, 45, 51, 19, 98, 68, 53, 0, 0, 224, 64,
            56, 5, 74, 7, 103, 97, 116, 101, 119, 97, 121, 82, 32, 189, 26, 199, 184, 218, 224,
            153, 239, 179, 212, 252, 188, 82, 56, 92, 96, 42, 86, 211, 112, 236, 39, 113, 1, 92,
            157, 241, 66, 4, 253, 232, 131,
        ],
        [
            8, 229, 153, 138, 206, 177, 48, 16, 10, 32, 35, 45, 51, 19, 98, 68, 53, 0, 0, 224, 64,
            56, 5, 74, 7, 103, 97, 116, 101, 119, 97, 121, 82, 32, 158, 226, 23, 203, 37, 225, 89,
            2, 1, 40, 167, 154, 210, 78, 210, 137, 53, 73, 176, 227, 109, 72, 120, 220, 61, 76,
            149, 219, 73, 107, 69, 28,
        ],
        [
            8, 223, 161, 138, 206, 177, 48, 16, 10, 32, 35, 45, 51, 19, 98, 68, 53, 0, 0, 224, 64,
            56, 5, 74, 7, 103, 97, 116, 101, 119, 97, 121, 82, 32, 204, 190, 205, 19, 81, 165, 69,
            171, 191, 239, 29, 52, 249, 56, 3, 219, 226, 216, 176, 95, 197, 249, 192, 206, 91, 124,
            164, 214, 142, 197, 219, 17,
        ],
        [
            8, 223, 161, 138, 206, 177, 48, 16, 10, 32, 35, 45, 51, 19, 98, 68, 53, 0, 0, 224, 64,
            56, 5, 74, 7, 103, 97, 116, 101, 119, 97, 121, 82, 32, 158, 226, 23, 203, 37, 225, 89,
            2, 1, 40, 167, 154, 210, 78, 210, 137, 53, 73, 176, 227, 109, 72, 120, 220, 61, 76,
            149, 219, 73, 107, 69, 28,
        ],
        [
            8, 248, 131, 136, 206, 177, 48, 16, 10, 32, 35, 45, 51, 19, 98, 68, 53, 0, 0, 224, 64,
            56, 5, 74, 7, 103, 97, 116, 101, 119, 97, 121, 82, 32, 193, 136, 202, 219, 53, 29, 141,
            221, 70, 66, 250, 246, 7, 69, 87, 254, 73, 101, 19, 91, 252, 208, 95, 149, 170, 195,
            106, 73, 107, 124, 98, 252,
        ],
        [
            8, 248, 131, 136, 206, 177, 48, 16, 10, 32, 35, 45, 51, 19, 98, 68, 53, 0, 0, 224, 64,
            56, 5, 74, 7, 103, 97, 116, 101, 119, 97, 121, 82, 32, 158, 226, 23, 203, 37, 225, 89,
            2, 1, 40, 167, 154, 210, 78, 210, 137, 53, 73, 176, 227, 109, 72, 120, 220, 61, 76,
            149, 219, 73, 107, 69, 28,
        ],
    ];

    #[test]
    fn decode_packet_reports() {
        for encoded in ENCODED_PROTOBUF_BYTES {
            let bytes = encoded.as_slice();
            let decoded = PacketRouterPacketReportV1::decode(bytes).unwrap();
            let PacketRouterPacketReportV1 {
                oui,
                net_id,
                gateway,
                payload_hash,
                ..
            } = decoded;

            println!(
                "oui={}, net_id={}, gateway={:#x?}, payload_hash={:#x?}",
                oui,
                net_id,
                &gateway[0..5],
                &payload_hash[0..5]
            );
        }
    }

    #[test]
    fn encode_bookkeeping_output() {
        let envelope = IotPacketVerifierBookkeeping {
            num_oui_entries: 1,
            num_gateway_entries: 1,
            ouis: [OuiBookkeeping {
                oui: 999,
                packet_count: 90909,
            }]
            .to_vec(),
            gateways: [GatewayBookkeeping {
                gateway: b"F0F0F0F0F0F0".to_vec(),
                packet_count: 808,
            }]
            .to_vec(),
            filename: b"s3/bucket/foo.dat".to_vec(),
        };
        let mut buf = vec![];
        envelope.encode(&mut buf).unwrap();
        println!("bytes={:x?}", &buf[0..20]);
    }

    #[test]
    fn update_counters() {
        for encoded in ENCODED_PROTOBUF_BYTES {
            let bytes = encoded.as_slice();
            let decoded = PacketRouterPacketReportV1::decode(bytes).unwrap();
            let ingest: PacketRouterPacketReportV1 = decoded;
            let mut counters = pv::PacketCounters::new();
            pv::update_counters(&ingest, &mut counters);
        }
    }

    #[cfg(feature = "sample-data")]
    mod generate_sample_data {
        extern crate async_compression;
        extern crate futures_util;
        extern crate tokio;

        use async_compression::tokio::write::GzipEncoder;
        use bytes::Bytes;
        use futures_util::sink::SinkExt;
        use tokio::{fs::OpenOptions, io::BufWriter};
        use tokio_util::codec::{length_delimited::LengthDelimitedCodec, FramedWrite};

        #[test]
        fn make_local_protobuf_files() {
            make_pb_files().unwrap();
        }

        /// Write a set of gzip'd Protobuf files and a single concatenated version.
        #[tokio::main]
        async fn make_pb_files() -> Result<(), Box<dyn std::error::Error>> {
            let cat_filename = format!("tests/HPR-report.stream.gz");
            let writer = GzipEncoder::new(BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(&cat_filename)
                    .await?,
            ));
            let mut transport = FramedWrite::new(writer, LengthDelimitedCodec::new());

            for (i, encoded) in super::ENCODED_PROTOBUF_BYTES.into_iter().enumerate() {
                let filename = format!("tests/HPR-report-{i:02x}.data.gz");
                let mut bytes = Bytes::from(encoded.to_vec());
                let buf = bytes.clone();
                match write_length_encoded(&filename, &mut bytes).await {
                    Ok(()) => (),
                    Err(e) => println!("failed: file={} {:?}", &filename, e),
                };
                transport.send(buf).await?;
            }
            Ok(())
        }

        /// Generate sample data files files for further exploration.
        /// Write length then payload similar to ../store/file_sink.rs
        // https://docs.rs/tokio-util/latest/tokio_util/codec/length_delimited/
        // For similar use cases, see also: Prost's Message
        async fn write_length_encoded(
            filename: &str,
            buf: &mut Bytes,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let writer = GzipEncoder::new(BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(&filename)
                    .await?,
            ));
            // Essentially, write a single u32 first:
            let mut transport = FramedWrite::new(writer, LengthDelimitedCodec::new());
            transport.send(buf.to_owned()).await?;
            Ok(())
        }
    }
}
