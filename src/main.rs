mod app;
mod data;
mod inference;
mod tui;
mod ui;

use app::App;
use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut terminal = tui::init()?;
    let mut app = App::new();
    let res = app.run(&mut terminal).await;
    
    tui::restore()?;

    if let Err(e) = res {
        println!("Error: {:?}", e);
    }

    Ok(())
}
