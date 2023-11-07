use std::{ops::Deref, sync::Arc};

use tokio::runtime::Handle;
use zookeeper_client::Client;

pub use zookeeper_client::{
    Acls, AddWatchMode, CreateMode, CreateOptions, CreateSequence, EventType, OneshotWatcher,
    PersistentWatcher, Stat, WatchedEvent,
};

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type Error = zookeeper_client::Error;

#[derive(Debug, Clone)]
pub struct Zookeeper {
    pub client: Client,
    runtime: Handle,
}

impl Deref for Zookeeper {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl Zookeeper {
    pub async fn connect(cluster: &str) -> Result<Self> {
        let runtime = Handle::try_current().expect("Must be called from within a tokio runtime");

        Ok(Self {
            client: Client::connect(cluster).await?,
            runtime,
        })
    }

    pub fn asyn(&self) -> &Client {
        &self.client
    }

    pub fn create(
        &self,
        path: &str,
        data: &[u8],
        options: &CreateOptions<'_>,
    ) -> Result<(Stat, CreateSequence)> {
        futures::executor::block_on(self.client.create(path, data, options))
    }

    pub fn delete(&self, path: &str, expected_version: Option<i32>) -> Result<(), Error> {
        futures::executor::block_on(self.client.delete(path, expected_version))
    }

    pub fn mkdir(&self, path: &str, options: &CreateOptions<'_>) -> Result<()> {
        futures::executor::block_on(self.client.mkdir(path, options))
    }

    pub fn get_data(&self, path: &str) -> Result<(Vec<u8>, Stat)> {
        futures::executor::block_on(self.client.get_data(path))
    }

    pub fn set_data(&self, path: &str, data: &[u8], expected_version: Option<i32>) -> Result<Stat> {
        futures::executor::block_on(self.client.set_data(path, data, expected_version))
    }

    pub fn list_children(&self, path: &str) -> Result<Vec<String>> {
        futures::executor::block_on(self.client.list_children(path))
    }

    pub fn list_and_watch_children(&self, path: &str) -> Result<(Vec<String>, OneshotWatcher)> {
        futures::executor::block_on(self.client.list_and_watch_children(path))
    }

    pub fn get_children(&self, path: &str) -> Result<(Vec<String>, Stat)> {
        futures::executor::block_on(self.client.get_children(path))
    }

    pub fn watch(&self, path: &str, mode: AddWatchMode) -> Result<Watcher> {
        futures::executor::block_on(self.client.watch(path, mode)).map(|watcher| Watcher {
            watcher,
            runtime: self.runtime.clone(),
        })
    }

    pub fn multi_watcher(&self, watchers: impl IntoIterator<Item = Watcher>) -> MultiWatcher {
        MultiWatcher::new(watchers.into_iter().map(|watcher| watcher.watcher))
    }
}

pub struct Watcher {
    watcher: PersistentWatcher,
    runtime: Handle,
}

impl Watcher {
    pub fn changed(&mut self) -> WatchedEvent {
        futures::executor::block_on(self.watcher.changed())
    }

    pub fn remove(self) -> Result<(), Error> {
        futures::executor::block_on(self.watcher.remove())
    }

    /// In order to call this function you must ensure that you're not running in a tokio runtime.
    /// Or else, run it in a `tokio::task::spawn_blocking` context.
    pub fn run_on_change(mut self, f: impl Fn(WatchedEvent) + Send + Sync + 'static) {
        let runtime = self.runtime.clone();
        self.runtime.spawn(async move {
            let f = Arc::new(f);
            loop {
                let event = self.watcher.changed().await;
                let f = f.clone();
                runtime.spawn_blocking(move || f(event)).await.unwrap();
            }
        });
    }
}

pub struct MultiWatcher {
    watchers: Vec<PersistentWatcher>,
}

#[derive(Debug)]
pub struct Entry {
    event: WatchedEvent,
    index: usize,
}

impl Deref for Entry {
    type Target = WatchedEvent;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

impl From<Entry> for WatchedEvent {
    fn from(value: Entry) -> Self {
        value.event
    }
}

impl MultiWatcher {
    fn new(watchers: impl IntoIterator<Item = PersistentWatcher>) -> Self {
        Self {
            watchers: watchers.into_iter().collect(),
        }
    }

    pub fn changed(&mut self) -> Entry {
        let future = futures::future::select_all(
            self.watchers
                .iter_mut()
                .map(|watcher| Box::pin(watcher.changed())),
        );
        let future = futures::executor::block_on(future);
        Entry {
            event: future.0,
            index: future.1,
        }
    }

    /// The `Entry` must come from the LAST `changed`, if not you
    /// may remove the wrong watcher or worse, cause a panic.
    pub fn remove(&mut self, entry: Entry) -> Result<()> {
        let watcher = self.watchers.remove(entry.index);

        futures::executor::block_on(watcher.remove())
    }

    /// The `Entry` must come from the LAST `changed`, if not you
    /// may remove the wrong watcher or worse, cause a panic.
    ///
    /// Can return an error if the old watcher couldn't be closed successfully,
    /// but the new watcher will be inserted anyway.
    pub fn replace(&mut self, entry: Entry, watcher: Watcher) -> Result<()> {
        let watcher_to_remove = std::mem::replace(&mut self.watchers[entry.index], watcher.watcher);

        futures::executor::block_on(watcher_to_remove.remove())
    }

    pub fn remove_all(self) -> Result<()> {
        let error = futures::executor::block_on(async {
            let mut error = None;
            for watcher in self.watchers {
                // We should still try to close all the watchers.
                if let Err(e) = watcher.remove().await {
                    error = Some(e);
                }
            }
            error
        });
        if let Some(error) = error {
            Err(error)
        } else {
            Ok(())
        }
    }
}
