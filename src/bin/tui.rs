use tokio::sync::mpsc;


#[tokio::main]
async fn main() {
    let (sx, rx) = mpsc::channel(100);
    let _spawn = tokio::spawn(async |sx| {
        let event = crossterm::event::read().expect("I forgot how to handle this, since it has been 3 months");
    }());
}
