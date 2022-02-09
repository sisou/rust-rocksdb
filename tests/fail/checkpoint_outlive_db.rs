extern crate ckb_rocksdb as rocksdb;

use rocksdb::{DB, checkpoint::Checkpoint};
use rocksdb::ops::Open;

fn main() {
    let _checkpoint = {
        let db = DB::open_default("foo").unwrap();
        Checkpoint::new(&db)
    };
}
