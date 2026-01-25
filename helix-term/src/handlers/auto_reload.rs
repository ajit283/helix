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

        let watcher = notify::recommended_watcher(move |res: Result<notify::Event, _>| {
            if let Ok(event) = res {
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
            Ok(w) => self.watcher = Some(w),
            Err(e) => log::warn!("Failed to create file watcher for auto-reload: {}", e),
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
                if let Some(ref mut watcher) = self.watcher {
                    if let Err(e) = watcher.watch(&path, RecursiveMode::NonRecursive) {
                        log::warn!("Failed to watch file {:?}: {}", path, e);
                    } else {
                        self.path_to_doc.insert(path.clone(), doc_id);
                        self.doc_to_path.insert(doc_id, path);
                    }
                }
                timeout
            }
            AutoReloadEvent::Unregister { doc_id } => {
                if let Some(path) = self.doc_to_path.remove(&doc_id) {
                    self.path_to_doc.remove(&path);
                    self.pending.remove(&path);
                    if let Some(ref mut watcher) = self.watcher {
                        if let Err(e) = watcher.unwatch(&path) {
                            log::warn!("Failed to unwatch file {:?}: {}", path, e);
                        }
                    }
                }
                timeout
            }
            AutoReloadEvent::FileChanged { path } => {
                // Canonicalize to match how helix stores paths
                let path = canonicalize(&path);
                if !self.path_to_doc.contains_key(&path) {
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
        for path in pending {
            let path_clone = path.clone();
            job::dispatch_blocking(move |editor, _| {
                // Check if auto_reload is enabled
                if !editor.config().auto_reload {
                    return;
                }

                // Find the document by path
                let Some(doc) = editor.document_by_path(&path_clone) else {
                    return;
                };

                let doc_id = doc.id();

                // Check if the document has unsaved modifications
                if doc.is_modified() {
                    editor.set_status(format!(
                        "\"{}\" changed on disk (buffer has unsaved changes)",
                        doc.display_name()
                    ));
                    return;
                }

                // Check if file content actually differs from buffer
                // (skip reload if same - likely our own save)
                if let Ok(file_content) = std::fs::read_to_string(&path_clone) {
                    if file_content == doc.text().to_string() {
                        return;
                    }
                }

                // Get a view for this document
                let view_ids: Vec<_> = doc.selections().keys().cloned().collect();
                if view_ids.is_empty() {
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

                        // Notify language servers about the change
                        if let Some(path) = doc_path {
                            editor
                                .language_servers
                                .file_event_handler
                                .file_changed(path);
                        }
                    }
                    Err(e) => {
                        editor.set_error(format!("Failed to reload: {}", e));
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
