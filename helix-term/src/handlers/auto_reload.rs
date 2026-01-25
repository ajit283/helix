use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    time::Duration,
};

use helix_event::{register_hook, send_blocking};
use helix_stdx::path::canonicalize;
use helix_view::{
    events::{DocumentDidClose, DocumentDidOpen},
    handlers::{AutoReloadEvent, Handlers},
    DocumentId,
};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;

use crate::job;

const COOLDOWN: Duration = Duration::from_millis(500);

pub(super) struct AutoReloadHandler {
    watcher: Option<RecommendedWatcher>,
    path_to_doc: HashMap<PathBuf, DocumentId>,
    doc_to_path: HashMap<DocumentId, PathBuf>,
    pending: HashSet<PathBuf>,
    /// Tracks when each path's cooldown expires
    cooldown_expires: HashMap<PathBuf, Instant>,
}

impl AutoReloadHandler {
    pub fn new() -> Self {
        Self {
            watcher: None,
            path_to_doc: HashMap::new(),
            doc_to_path: HashMap::new(),
            pending: HashSet::new(),
            cooldown_expires: HashMap::new(),
        }
    }

    fn init_watcher(&mut self, tx: Sender<AutoReloadEvent>) {
        if self.watcher.is_some() {
            return;
        }

        log::info!("auto-reload: initializing file watcher");
        let watcher = notify::recommended_watcher(move |res: Result<notify::Event, _>| {
            if let Ok(event) = res {
                log::trace!(
                    "auto-reload: notify event kind={:?} paths={:?}",
                    event.kind,
                    event.paths
                );
                if matches!(
                    event.kind,
                    notify::EventKind::Modify(_) | notify::EventKind::Create(_)
                ) {
                    for path in event.paths {
                        // Send through the main channel to wake up the handler
                        send_blocking(&tx, AutoReloadEvent::FileChanged { path });
                    }
                }
            }
        });

        match watcher {
            Ok(w) => {
                log::info!("auto-reload: file watcher ready");
                self.watcher = Some(w);
            }
            Err(e) => log::warn!("auto-reload: failed to create file watcher: {}", e),
        }
    }
}

impl helix_event::AsyncHook for AutoReloadHandler {
    type Event = AutoReloadEvent;

    fn handle_event(&mut self, event: Self::Event, timeout: Option<Instant>) -> Option<Instant> {
        match event {
            AutoReloadEvent::Init { tx } => {
                self.init_watcher(tx);
                timeout
            }
            AutoReloadEvent::Register { doc_id, path } => {
                // Canonicalize to ensure consistent path matching
                let path = canonicalize(&path);
                log::debug!("auto-reload: register doc_id={:?} path={:?}", doc_id, path);
                if let Some(ref mut watcher) = self.watcher {
                    if let Err(e) = watcher.watch(&path, RecursiveMode::NonRecursive) {
                        log::warn!("auto-reload: failed to watch file {:?}: {}", path, e);
                    } else {
                        self.path_to_doc.insert(path.clone(), doc_id);
                        self.doc_to_path.insert(doc_id, path);
                    }
                } else {
                    log::warn!("auto-reload: register requested before watcher init");
                }
                timeout
            }
            AutoReloadEvent::Unregister { doc_id } => {
                log::debug!("auto-reload: unregister doc_id={:?}", doc_id);
                if let Some(path) = self.doc_to_path.remove(&doc_id) {
                    self.path_to_doc.remove(&path);
                    self.pending.remove(&path);
                    if let Some(ref mut watcher) = self.watcher {
                        if let Err(e) = watcher.unwatch(&path) {
                            log::warn!("auto-reload: failed to unwatch file {:?}: {}", path, e);
                        }
                    }
                }
                timeout
            }
            AutoReloadEvent::FileChanged { path } => {
                // Canonicalize to match how helix stores paths
                let path = canonicalize(&path);
                log::trace!("auto-reload: file changed path={:?}", path);
                if !self.path_to_doc.contains_key(&path) {
                    log::trace!("auto-reload: ignoring event for unregistered path");
                    return timeout;
                }

                // Already pending - don't change timeout (avoid extending due to duplicate events)
                if self.pending.contains(&path) {
                    return timeout;
                }

                let now = Instant::now();
                self.pending.insert(path.clone());

                if let Some(&expires) = self.cooldown_expires.get(&path) {
                    if now < expires {
                        // In cooldown - schedule for when cooldown expires
                        log::trace!(
                            "auto-reload: in cooldown, scheduling for {:?}",
                            expires
                        );
                        return Some(expires);
                    }
                }

                // Not in cooldown - process immediately, start cooldown
                self.cooldown_expires.insert(path, now + COOLDOWN);
                Some(now)
            }
        }
    }

    fn finish_debounce(&mut self) {
        // Clear expired cooldowns
        let now = Instant::now();
        self.cooldown_expires.retain(|_, expires| *expires > now);

        let pending: Vec<PathBuf> = self.pending.drain().collect();
        if !pending.is_empty() {
            log::debug!("auto-reload: processing {} pending paths", pending.len());
        }
        for path in pending {
            let path_clone = path.clone();
            job::dispatch_blocking(move |editor, _| {
                // Check if auto_reload is enabled
                if !editor.config().auto_reload {
                    log::debug!("auto-reload: disabled by config, skipping reload");
                    return;
                }

                // Find the document by path
                let Some(doc) = editor.document_by_path(&path_clone) else {
                    log::debug!("auto-reload: no document found for path {:?}", path_clone);
                    return;
                };

                let doc_id = doc.id();

                // Check if the document has unsaved modifications
                if doc.is_modified() {
                    editor.set_status(format!(
                        "\"{}\" changed on disk (buffer has unsaved changes)",
                        doc.display_name()
                    ));
                    log::debug!(
                        "auto-reload: doc has unsaved changes, skipping reload path={:?}",
                        path_clone
                    );
                    return;
                }

                // Check if file content actually differs from buffer
                // (skip reload if same - likely our own save)
                if let Ok(file_content) = std::fs::read_to_string(&path_clone) {
                    if file_content == doc.text().to_string() {
                        log::trace!(
                            "auto-reload: content unchanged, re-registering watch"
                        );
                        // Atomic replace (temp write + rename) can swap inodes without changing
                        // content; re-register to keep the watch valid on Linux.
                        if let Some(path) = doc.path().cloned() {
                            send_blocking(
                                &editor.handlers.auto_reload,
                                AutoReloadEvent::Unregister { doc_id },
                            );
                            send_blocking(
                                &editor.handlers.auto_reload,
                                AutoReloadEvent::Register { doc_id, path },
                            );
                        }
                        return;
                    }
                }

                // Get a view for this document
                let view_ids: Vec<_> = doc.selections().keys().cloned().collect();
                if view_ids.is_empty() {
                    log::debug!("auto-reload: no views for document, skipping reload");
                    return;
                }

                let view_id = view_ids[0];

                // Get mutable references and reload
                let doc = match editor.documents.get_mut(&doc_id) {
                    Some(doc) => doc,
                    None => return,
                };

                // Store display name and path before mutable operations
                let display_name = doc.display_name().to_string();
                let doc_path = doc.path().cloned();

                let view = editor.tree.get_mut(view_id);

                // Sync view with document history
                view.sync_changes(doc);

                let reload_result = doc.reload(view, &editor.diff_providers);

                match reload_result {
                    Ok(()) => {
                        editor.set_status(format!("Reloaded \"{}\"", display_name));
                        log::info!("auto-reload: reloaded {:?}", doc_path);

                        // Notify language servers about the change
                        if let Some(path) = doc_path.clone() {
                            editor
                                .language_servers
                                .file_event_handler
                                .file_changed(path);
                        }

                        // Re-register the watch to handle inode changes on Linux
                        // When files are atomically replaced (temp write + rename), the inode
                        // changes and inotify watches become invalid. Re-registering ensures
                        // we watch the current inode.
                        if let Some(path) = doc_path {
                            send_blocking(
                                &editor.handlers.auto_reload,
                                AutoReloadEvent::Unregister { doc_id },
                            );
                            send_blocking(
                                &editor.handlers.auto_reload,
                                AutoReloadEvent::Register { doc_id, path },
                            );
                        }
                    }
                    Err(e) => {
                        editor.set_error(format!("Failed to reload: {}", e));
                        log::warn!("auto-reload: reload failed: {}", e);
                    }
                }
            });
        }
    }
}

pub(super) fn register_hooks(handlers: &Handlers) {
    // Initialize the watcher with the channel sender
    send_blocking(
        &handlers.auto_reload,
        AutoReloadEvent::Init {
            tx: handlers.auto_reload.clone(),
        },
    );

    let tx = handlers.auto_reload.clone();
    register_hook!(move |event: &mut DocumentDidOpen<'_>| {
        if !event.editor.config().auto_reload {
            return Ok(());
        }

        let doc = event.editor.document(event.doc).unwrap();
        if let Some(path) = doc.path() {
            send_blocking(
                &tx,
                AutoReloadEvent::Register {
                    doc_id: event.doc,
                    path: path.to_path_buf(),
                },
            );
        }
        Ok(())
    });

    let tx = handlers.auto_reload.clone();
    register_hook!(move |event: &mut DocumentDidClose<'_>| {
        send_blocking(
            &tx,
            AutoReloadEvent::Unregister {
                doc_id: event.doc.id(),
            },
        );
        Ok(())
    });
}
