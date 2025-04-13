use crate::events::OnModeSwitch;
use helix_event::register_hook;
use helix_view::document::Mode;
use interprocess::local_socket::tokio::prelude::*;
use interprocess::local_socket::{tokio::Stream, GenericFilePath, ListenerOptions};
use std::io;
use std::path::PathBuf;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::UnboundedSender;

pub async fn start_ipc_server(sender: UnboundedSender<PathBuf>) -> io::Result<()> {
    let pid = std::process::id();
    let socket_path = format!("/tmp/helix-{}.sock", pid);
    // let socket_path = "/tmp/helix.sock";
    let name = socket_path
        .to_fs_name::<GenericFilePath>()
        .expect("Failed to resolve socket name");

    let meta_path = format!("/tmp/helix-{}.meta", pid);
    std::fs::write(&meta_path, "")?;

    register_hook!(move |event: &mut OnModeSwitch<'_, '_>| {
        if event.old_mode == Mode::Insert {
            if let Err(e) = std::fs::write(&meta_path, "") {
                eprintln!("meta write error: {e}");
            }
        }
        Ok(())
    });

    let opts = ListenerOptions::new().name(name);

    let listener = opts.create_tokio().map_err(|e| {
        eprintln!("❌ IPC bind error: {}", e);
        e
    })?;

    tokio::spawn(async move {
        loop {
            let conn = match listener.accept().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("⚠️ IPC accept error: {e}");
                    continue;
                }
            };
            let sender = sender.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_conn(conn, sender).await {
                    eprintln!("❌ Error handling IPC connection: {e}");
                }
            });
        }
    });

    Ok(())
}

async fn handle_conn(conn: Stream, sender: UnboundedSender<PathBuf>) -> io::Result<()> {
    let mut reader = BufReader::new(conn);
    let mut line = String::new();

    let bytes = reader.read_line(&mut line).await?;
    if bytes > 0 {
        let path = PathBuf::from(line.trim());
        sender.send(path).ok(); // Ignore failure to send
    }

    Ok(())
}

pub fn cleanup_ipc_artifacts() {
    let pid = std::process::id();
    let _ = std::fs::remove_file(format!("/tmp/helix-{}.sock", pid));
    let _ = std::fs::remove_file(format!("/tmp/helix-{}.meta", pid));
}
