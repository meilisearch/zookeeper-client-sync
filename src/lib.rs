use std::ops::Deref;

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
        self.runtime
            .block_on(self.client.create(path, data, options))
    }

    pub fn delete(&self, path: &str, expected_version: Option<i32>) -> Result<(), Error> {
        self.runtime
            .block_on(self.client.delete(path, expected_version))
    }

    pub fn mkdir(&self, path: &str, options: &CreateOptions<'_>) -> Result<()> {
        self.runtime.block_on(self.client.mkdir(path, options))
    }

    pub fn get_data(&self, path: &str) -> Result<(Vec<u8>, Stat)> {
        self.runtime.block_on(self.client.get_data(path))
    }

    pub fn set_data(&self, path: &str, data: &[u8], expected_version: Option<i32>) -> Result<Stat> {
        self.runtime
            .block_on(self.client.set_data(path, data, expected_version))
    }

    pub fn list_children(&self, path: &str) -> Result<Vec<String>> {
        self.runtime.block_on(self.client.list_children(path))
    }

    pub fn list_and_watch_children(&self, path: &str) -> Result<(Vec<String>, OneshotWatcher)> {
        self.runtime
            .block_on(self.client.list_and_watch_children(path))
    }

    pub fn get_children(&self, path: &str) -> Result<(Vec<String>, Stat)> {
        self.runtime.block_on(self.client.get_children(path))
    }

    pub fn watch(&self, path: &str, mode: AddWatchMode) -> Result<Watcher> {
        self.runtime
            .block_on(self.client.watch(path, mode))
            .map(|watcher| Watcher {
                watcher,
                runtime: self.runtime.clone(),
            })
    }

    pub fn multi_watcher(&self, watchers: impl IntoIterator<Item = Watcher>) -> MultiWatcher {
        MultiWatcher::new(
            self.runtime.clone(),
            watchers.into_iter().map(|watcher| watcher.watcher),
        )
    }
}

pub struct Watcher {
    watcher: PersistentWatcher,
    runtime: Handle,
}

impl Watcher {
    pub fn changed(&mut self) -> WatchedEvent {
        self.runtime.block_on(self.watcher.changed())
    }

    pub fn remove(self) -> Result<(), Error> {
        self.runtime.block_on(self.watcher.remove())
    }

    pub fn run_on_change(mut self, f: impl Fn(WatchedEvent) + Send + Sync + 'static) {
        self.runtime.spawn(async move {
            loop {
                let event = self.watcher.changed().await;
                f(event)
            }
        });
    }
}

pub struct MultiWatcher {
    watchers: Vec<PersistentWatcher>,
    runtime: Handle,
}

impl MultiWatcher {
    fn new(runtime: Handle, watchers: impl IntoIterator<Item = PersistentWatcher>) -> Self {
        Self {
            watchers: watchers.into_iter().collect(),
            runtime,
        }
    }

    pub fn changed(&mut self) -> WatchedEvent {
        let future = futures::future::select_all(
            self.watchers
                .iter_mut()
                .map(|watcher| Box::pin(watcher.changed())),
        );
        self.runtime.block_on(async { future.await.0 })
    }

    pub fn remove(self) -> Result<(), Error> {
        let error = self.runtime.block_on(async {
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
