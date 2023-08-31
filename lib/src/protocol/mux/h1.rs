use sozu_command::ready::Ready;

use crate::{
    protocol::mux::{BackendStatus, Context, Endpoint, GlobalStreamId, MuxResult, Position},
    socket::SocketHandler,
    Readiness,
};

pub struct ConnectionH1<Front: SocketHandler> {
    pub position: Position,
    pub readiness: Readiness,
    pub socket: Front,
    /// note: a Server H1 will always reference stream 0, but a client can reference any stream
    pub stream: GlobalStreamId,
}

impl<Front: SocketHandler> std::fmt::Debug for ConnectionH1<Front> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionH1")
            .field("position", &self.position)
            .field("readiness", &self.readiness)
            .field("socket", &self.socket.socket_ref())
            .field("stream", &self.stream)
            .finish()
    }
}

impl<Front: SocketHandler> ConnectionH1<Front> {
    pub fn readable<E>(&mut self, context: &mut Context, mut endpoint: E) -> MuxResult
    where
        E: Endpoint,
    {
        println!("======= MUX H1 READABLE");
        let stream = &mut context.streams[self.stream];
        let parts = stream.split(&self.position);
        let kawa = parts.rbuffer;
        let (size, status) = self.socket.socket_read(kawa.storage.space());
        println!("  size: {size}, status: {status:?}");
        if size > 0 {
            kawa.storage.fill(size);
        } else {
            self.readiness.event.remove(Ready::READABLE);
            return MuxResult::Continue;
        }
        // match status {
        //     SocketResult::Continue => {}
        //     SocketResult::Closed => todo!(),
        //     SocketResult::Error => todo!(),
        //     SocketResult::WouldBlock => self.readiness.event.remove(Ready::READABLE),
        // }
        let was_initial = kawa.is_initial();
        kawa::h1::parse(kawa, parts.context);
        kawa::debug_kawa(kawa);
        if kawa.is_error() {
            return MuxResult::Close(self.stream);
        }
        if kawa.is_terminated() {
            self.readiness.interest.remove(Ready::READABLE);
        }
        if was_initial && kawa.is_main_phase() {
            match self.position {
                Position::Client(_) => endpoint
                    .readiness_mut(stream.token.unwrap())
                    .interest
                    .insert(Ready::WRITABLE),
                Position::Server => return MuxResult::Connect(self.stream),
            };
        }
        MuxResult::Continue
    }
    pub fn writable<E>(&mut self, context: &mut Context, mut endpoint: E) -> MuxResult
    where
        E: Endpoint,
    {
        println!("======= MUX H1 WRITABLE");
        let stream = &mut context.streams[self.stream];
        let kawa = stream.wbuffer(&self.position);
        kawa.prepare(&mut kawa::h1::BlockConverter);
        kawa::debug_kawa(kawa);
        let bufs = kawa.as_io_slice();
        if bufs.is_empty() {
            self.readiness.interest.remove(Ready::WRITABLE);
            return MuxResult::Continue;
        }
        let (size, status) = self.socket.socket_write_vectored(&bufs);
        println!("  size: {size}, status: {status:?}");
        if size > 0 {
            kawa.consume(size);
            // self.backend_readiness.interest.insert(Ready::READABLE);
        } else {
            self.readiness.event.remove(Ready::WRITABLE);
        }
        if kawa.is_terminated() && kawa.is_completed() {
            match self.position {
                Position::Client(_) => self.readiness.interest.insert(Ready::READABLE),
                Position::Server => {
                    endpoint.end_stream(stream.token.unwrap(), self.stream, context)
                }
            }
        }
        MuxResult::Continue
    }

    pub fn close<E>(&mut self, context: &mut Context, mut endpoint: E)
    where
        E: Endpoint,
    {
        match self.position {
            Position::Client(BackendStatus::KeepAlive(_))
            | Position::Client(BackendStatus::Disconnecting) => {
                println!("close detached client ConnectionH1");
                return;
            }
            Position::Client(BackendStatus::Connecting(_)) => todo!("reconnect"),
            Position::Client(_) => {}
            Position::Server => unreachable!(),
        }
        endpoint.end_stream(
            context.streams[self.stream].token.unwrap(),
            self.stream,
            context,
        )
    }

    pub fn end_stream(&mut self, stream: usize, context: &mut Context) {
        assert_eq!(stream, self.stream);
        let stream_context = &mut context.streams[stream].context;
        println!("end H1 stream {stream}: {stream_context:#?}");
        self.stream = usize::MAX;
        let mut owned_position = Position::Server;
        std::mem::swap(&mut owned_position, &mut self.position);
        match owned_position {
            Position::Client(BackendStatus::Connected(cluster_id))
            | Position::Client(BackendStatus::Connecting(cluster_id)) => {
                self.position = if stream_context.keep_alive_backend {
                    Position::Client(BackendStatus::KeepAlive(cluster_id))
                } else {
                    Position::Client(BackendStatus::Disconnecting)
                }
            }
            Position::Client(BackendStatus::KeepAlive(_))
            | Position::Client(BackendStatus::Disconnecting) => unreachable!(),
            Position::Server => todo!(),
        }
    }

    pub fn start_stream(&mut self, stream: usize, context: &mut Context) {
        println!("start H1 stream {stream}");
        self.stream = stream;
        let mut owned_position = Position::Server;
        std::mem::swap(&mut owned_position, &mut self.position);
        match owned_position {
            Position::Client(BackendStatus::KeepAlive(cluster_id)) => {
                self.position = Position::Client(BackendStatus::Connecting(cluster_id))
            }
            Position::Server => unreachable!(),
            _ => {
                self.position = owned_position;
            }
        }
    }
}