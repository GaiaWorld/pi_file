extern crate npnc;


extern crate pi_file;
extern crate pi_atom;

use std::thread;
use std::sync::Arc;

use pi_file::fs_monitor::{FSMonitorOptions, FSListener, FSMonitor};
use pi_atom::Atom;


#[test]
fn test_fs_monitor() {
    let listener = FSListener(Arc::new(|event| {
        println!("!!!!!!event: {:?}", event);
    }));
    let mut monitor = FSMonitor::new(FSMonitorOptions::Dir(Atom::from("test"), true, 3000), listener);
    if let Ok(_) = monitor.run() {
        loop {
            thread::sleep(std::time::Duration::from_millis(1000));
        }
    }
}
