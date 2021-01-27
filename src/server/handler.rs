use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tracing::instrument;

use super::{connection::Connection, shutdown::Shutdown};
use std::io::Result;

/// Per-connection handler. Reads requests from `connection` and applies the
/// commands to `db`.
#[derive(Debug)]
pub struct Handler {
    /// Shared database handle.
    ///
    /// When a command is received from `connection`, it is applied with `db`.
    /// The implementation of the command is in the `cmd` module. Each command
    /// will need to interact with `db` in order to complete the work.
    // db: Db,

    /// The TCP connection decorated with the protocol encoder / decoder
    /// implemented using a buffered `TcpStream`.
    ///
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    pub connection: Connection,

    /// Max connection semaphore.
    ///
    /// When the handler is dropped, a permit is returned to this semaphore. If
    /// the listener is waiting for connections to close, it will be notified of
    /// the newly available permit and resume accepting connections.
    pub limit_connections: Arc<Semaphore>,

    /// Listen for shutdown notifications.
    ///
    /// A wrapper around the `broadcast::Receiver` paired with the sender in
    /// `Listener`. The connection handler processes requests from the
    /// connection until the peer disconnects **or** a shutdown notification is
    /// received from `shutdown`. In the latter case, any in-flight work being
    /// processed for the peer is continued until it reaches a safe state, at
    /// which point the connection is terminated.
    pub shutdown: Shutdown,

    /// Not used directly. Instead, when `Handler` is dropped...?
    pub _shutdown_complete: mpsc::Sender<()>,
}

impl Handler {
    /// Process a single connection.
    ///
    /// Request frames are read from the socket and processed. Responses are
    /// written back to the socket.
    ///
    /// Currently, pipelining is not implemented. Pipelining is the ability to
    /// process more than one request concurrently per connection without
    /// interleaving frames. See for more details:
    /// https://redis.io/topics/pipelining
    ///
    /// When the shutdown signal is received, the connection is processed until
    /// it reaches a safe state, at which point it is terminated.
    #[instrument(skip(self))]
    pub async fn run(&mut self) -> Result<()> {
        // As long as the shutdown signal has not been received, try to read a
        // new request frame.
        while !self.shutdown.is_shutdown() {
            // While reading a request frame, also listen for the shutdown
            // signal.
            //let maybe_frame = tokio::select! {
            //res = self.connection.read_frame() => res?,
            //_ = self.shutdown.recv() => {
            //// If a shutdown signal is received, return from `run`.
            //// This will result in the task terminating.
            //return Ok(());
            //}
            //};

            //// If `None` is returned from `read_frame()` then the peer closed
            //// the socket. There is no further work to do and the task can be
            //// terminated.
            //let frame = match maybe_frame {
            //Some(frame) => frame,
            //None => return Ok(()),
            //};

            // Convert the frame into a command struct. This returns an
            // error if the frame is not a valid command or it is an
            // unsupported command.
            // let cmd = Command::from_frame(frame)?;

            // Logs the `cmd` object. The syntax here is a shorthand provided by
            // the `tracing` crate. It can be thought of as similar to:
            //
            // ```
            // debug!(cmd = format!("{:?}", cmd));
            // ```
            //
            // `tracing` provides structured logging, so information is "logged"
            // as key-value pairs.
            // debug!(?cmd);

            // Perform the work needed to apply the command. This may mutate the
            // database state as a result.
            //
            // The connection is passed into the apply function which allows the
            // command to write response frames directly to the connection. In
            // the case of pub/sub, multiple frames may be send back to the
            // peer.
            // cmd.apply(/* &self.db, */ &mut self.connection, &mut self.shutdown)
            //    .await?;
        }

        Ok(())
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        // Add a permit back to the semaphore.
        //
        // Doing so unblocks the listener if the max number of
        // connections has been reached.
        //
        // This is done in a `Drop` implementation in order to guarantee that
        // the permit is added even if the task handling the connection panics.
        // If `add_permit` was called at the end of the `run` function and some
        // bug causes a panic. The permit would never be returned to the
        // semaphore.
        self.limit_connections.add_permits(1);
    }
}
