#[derive(Debug)]
pub struct EntryWithMetaData {
    pub entry: Entry,
    pub meta_data: MetaData,
}

impl EntryWithMetaData {
    pub fn new(entry: Entry, meta_data: MetaData) -> Self {
        EntryWithMetaData { entry, meta_data }
    }
}
