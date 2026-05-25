use bevy::app::{PluginGroup, PluginGroupBuilder};
use bevy_matchbox::MatchboxSocket;
use bevy_matchbox::matchbox_socket::{ChannelConfig, Packet, PeerId, WebRtcChannel};
use std::collections::HashMap;
use bevy_replicon::postcard;
use bevy_replicon::prelude::{Channel, RepliconChannels};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

//Required to communicate which peer is the host before we start using replicon
pub(super) const SYSTEM_CHANNEL_ID: usize = 0;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(super) enum SystemChannelMessage {
    ConnectedToHost,
    HostRequestsDisconnect,
    ClientDisconnects,
}

pub struct RepliconMatchboxPlugins;

impl PluginGroup for RepliconMatchboxPlugins {
    fn build(self) -> PluginGroupBuilder {
        let mut group = PluginGroupBuilder::start::<Self>();

        #[cfg(feature = "server")]
        {
            use crate::server::RepliconMatchboxServerPlugin;
            group = group.add(RepliconMatchboxServerPlugin);
        }

        #[cfg(feature = "client")]
        {
            use crate::client::RepliconMatchboxClientPlugin;
            group = group.add(RepliconMatchboxClientPlugin);
        }

        group
    }
}
pub(crate) trait RepliconChannelsExt<'a> {
    type Iter: Iterator<Item = &'a Channel>;

    fn all_channels(&'a self) -> Self::Iter;
}

impl<'a> RepliconChannelsExt<'a> for RepliconChannels {
    type Iter = std::iter::Chain<std::slice::Iter<'a, Channel>, std::slice::Iter<'a, Channel>>;
    fn all_channels(&'a self) -> Self::Iter {
        self.server_channels()
            .iter()
            .chain(self.client_channels().iter())
    }
}

pub(super) fn create_matchbox_socket(
    room_url: impl Into<String>,
    replicon_channels: &RepliconChannels,
) -> MatchboxSocket {
    let mut web_rtc_socket = bevy_matchbox::matchbox_socket::WebRtcSocketBuilder::new(room_url);
    //add system channel
    web_rtc_socket = web_rtc_socket.add_reliable_channel();
    for &channel in replicon_channels.all_channels() {
        match channel {
            Channel::Unreliable => {
                web_rtc_socket = web_rtc_socket.add_unreliable_channel();
            }
            Channel::Unordered => {
                web_rtc_socket = web_rtc_socket.add_channel(ChannelConfig {
                    ordered: false,
                    max_retransmits: None,
                });
            }
            Channel::Ordered => {
                web_rtc_socket = web_rtc_socket.add_reliable_channel();
            }
        };
    }
    let socket = web_rtc_socket.build();
    MatchboxSocket::from(socket)
}

#[cfg(feature = "server")]
pub(super) fn uuid_to_u64_truncated(peer_id: PeerId) -> u64 {
    let bytes = peer_id.0.as_bytes();
    u64::from_le_bytes(bytes[0..8].try_into().unwrap())
}

// Fragmentation. WebRTC/SCTP rejects any single message larger than its max
// message size (64 KiB by default), but replicon hands large reliable messages
// - chiefly the initial replication snapshot of a populated world - to the
// backend whole, expecting it to split them (renet does). So we frame each
// outbound message and split oversized ones across packets, reassembling on
// receipt. The leading frame byte also serves the non-empty-message guard the
// old marker byte provided (matchbox drops zero-length packets).
//
// Reassembly assumes in-order delivery, which holds for the reliable channels
// where over-size messages occur. Messages on unreliable / unordered channels
// are small (replicon caps them at the MTU) and always travel as one WHOLE
// packet, so they never touch the reassembly buffer.

/// Max payload bytes per packet - well under SCTP's 64 KiB ceiling.
const MAX_FRAGMENT_PAYLOAD: usize = 16 * 1024;

const FRAME_WHOLE: u8 = 0; // entire message in this one packet
const FRAME_PART: u8 = 1; // a non-final fragment; more follow
const FRAME_LAST: u8 = 2; // the final fragment of a multi-packet message

/// Per-`(peer, channel)` accumulator for in-flight multi-packet messages.
pub(super) type FragmentBuffers = HashMap<(PeerId, usize), Vec<u8>>;

/// Send `data` to `peer` on `channel`, splitting it into framed packets when
/// it exceeds [`MAX_FRAGMENT_PAYLOAD`]. Reassembled by [`reassemble`].
pub(super) fn send_message(channel: &mut WebRtcChannel, peer: PeerId, data: &[u8]) {
    if data.len() <= MAX_FRAGMENT_PAYLOAD {
        channel.send(frame(FRAME_WHOLE, data), peer);
        return;
    }
    let mut chunks = data.chunks(MAX_FRAGMENT_PAYLOAD).peekable();
    while let Some(chunk) = chunks.next() {
        let header = if chunks.peek().is_some() {
            FRAME_PART
        } else {
            FRAME_LAST
        };
        channel.send(frame(header, chunk), peer);
    }
}

fn frame(header: u8, payload: &[u8]) -> Packet {
    let mut buf = Vec::with_capacity(payload.len() + 1);
    buf.push(header);
    buf.extend_from_slice(payload);
    buf.into()
}

/// Feed a received packet into the `(peer, channel)` reassembly buffer.
/// Returns the complete replicon message once its final fragment arrives, or
/// `None` while a multi-packet message is still being assembled.
pub(super) fn reassemble(
    buffers: &mut FragmentBuffers,
    peer: PeerId,
    channel: usize,
    packet: &[u8],
) -> Option<Bytes> {
    let (&header, payload) = packet.split_first()?;
    match header {
        FRAME_WHOLE => Some(Bytes::copy_from_slice(payload)),
        FRAME_PART => {
            buffers
                .entry((peer, channel))
                .or_default()
                .extend_from_slice(payload);
            None
        }
        FRAME_LAST => {
            let mut message = buffers.remove(&(peer, channel)).unwrap_or_default();
            message.extend_from_slice(payload);
            Some(Bytes::from(message))
        }
        _ => None, // unknown frame header; drop
    }
}

pub(super) fn to_packet<'a, T: Serialize>(msg: &T, buf: &'a mut [u8]) -> &'a [u8] {
    use bevy_replicon::postcard::to_slice;
    to_slice(msg, buf).expect("serialize failed")
}

pub(super) fn from_packet<'a, T: Deserialize<'a>>(
    data: &'a [u8],
) -> bevy::prelude::Result<T, postcard::Error> {
    postcard::from_bytes(data)
}

#[test]
fn test_packaging() {
    let messages = [
        SystemChannelMessage::ConnectedToHost,
        SystemChannelMessage::HostRequestsDisconnect,
    ];
    for msg in messages.iter() {
        let mut buf = [0u8; 1];
        let p = to_packet(&msg, &mut buf);
        let deserialized: SystemChannelMessage = from_packet(p).unwrap();
        assert_eq!(*msg, deserialized);
    }
}
